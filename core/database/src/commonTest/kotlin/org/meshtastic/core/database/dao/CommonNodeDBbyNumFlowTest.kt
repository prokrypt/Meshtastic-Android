/*
 * Copyright (c) 2026 Meshtastic LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.meshtastic.core.database.dao

import androidx.room3.executeSQL
import androidx.room3.useReaderConnection
import androidx.room3.useWriterConnection
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteDriver
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import okio.ByteString.Companion.toByteString
import org.meshtastic.core.database.BusyTimeoutSQLiteDriver
import org.meshtastic.core.database.MeshtasticDatabase
import org.meshtastic.core.database.entity.MetadataEntity
import org.meshtastic.core.database.entity.MyNodeEntity
import org.meshtastic.core.database.entity.NodeEntity
import org.meshtastic.core.database.entity.NodeWithRelations
import org.meshtastic.core.database.getInMemoryDatabaseBuilder
import org.meshtastic.core.testing.setupTestContext
import org.meshtastic.proto.DeviceMetadata
import org.meshtastic.proto.DeviceMetrics
import org.meshtastic.proto.EnvironmentMetrics
import org.meshtastic.proto.HardwareModel
import org.meshtastic.proto.Position
import org.meshtastic.proto.Telemetry
import org.meshtastic.proto.User
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * [observeNodeDBbyNum] must emit exactly what [NodeInfoDao.nodeDBbyNum] emits while decoding only changed rows.
 *
 * Full node rows are counted at the SQLite driver: every row a `SELECT * FROM nodes` statement steps over is a row
 * whose protobuf blobs Room decodes.
 */
abstract class CommonNodeDBbyNumFlowTest {
    private lateinit var database: MeshtasticDatabase
    private lateinit var dao: NodeInfoDao
    private val fullNodeRowsRead = atomic(0)
    private var heardSeq = 0

    @BeforeTest
    fun setUp() {
        setupTestContext()
        database =
            getInMemoryDatabaseBuilder()
                .setDriver(
                    CountingDriver(BusyTimeoutSQLiteDriver(BundledSQLiteDriver())) {
                        fullNodeRowsRead.incrementAndGet()
                    },
                )
                .build()
        dao = database.nodeInfoDao()
    }

    @AfterTest
    fun tearDown() {
        database.close()
    }

    @Test
    fun aPacketFromOneNodeDecodesOneRowInsteadOfTheWholeTable() = runTest {
        seedMesh()

        // The upsert itself reads the sender's existing row once to verify it; count that separately.
        val upsertOwnReads = readsPerUpsert(source = null)
        val baselineReads = readsPerUpsert { dao.nodeDBbyNum() } - upsertOwnReads
        val incrementalReads = readsPerUpsert { dao.observeNodeDBbyNum() } - upsertOwnReads

        println(
            "Full node rows decoded by the observer for $UPSERTS upserts into a $MESH_SIZE-node table: " +
                "nodeDBbyNum=$baselineReads, observeNodeDBbyNum=$incrementalReads",
        )
        assertTrue(baselineReads >= MESH_SIZE * UPSERTS, "the old flow re-reads the whole table per upsert")
        assertEquals(UPSERTS, incrementalReads, "the new flow reads only the sender's row per upsert")
    }

    @Test
    fun emissionsMatchTheFullQueryAcrossEveryKindOfWrite() = runTest {
        seedMesh()
        val latest = collectLatest(dao.observeNodeDBbyNum())
        awaitMatchesFullQuery(latest)

        dao.upsert(meshNode(3).copy(lastHeard = BASE_TIME + 10_000, snr = 1.5f))
        awaitMatchesFullQuery(latest)

        dao.upsert(meshNode(MESH_SIZE + 1))
        awaitMatchesFullQuery(latest)

        dao.setNodeNotes(5, "on the hill")
        awaitMatchesFullQuery(latest)

        dao.setNodeDisplayName(6, "Base camp")
        awaitMatchesFullQuery(latest)

        dao.upsert(MetadataEntity(7, DeviceMetadata.Builder().also { it.firmware_version = "2.8.0" }.build()))
        awaitMatchesFullQuery(latest)

        dao.upsert(MetadataEntity(7, DeviceMetadata.Builder().also { it.firmware_version = "2.8.1" }.build()))
        awaitMatchesFullQuery(latest)

        // A different local node moves to the front of the ordering without its own row changing. Room re-emits
        // neither this flow nor nodeDBbyNum for a my_node write alone, so the next node write carries the new order.
        dao.clearMyNodeInfo()
        dao.setMyNodeInfo(myNode(MESH_SIZE - 1))
        dao.setNodeNotes(8, "after the local node changed")
        awaitMatchesFullQuery(latest)

        dao.deleteNodeAndMetadata(7)
        awaitMatchesFullQuery(latest)

        dao.markAllHeardOnCurrentLora()
        awaitMatchesFullQuery(latest)

        dao.clearNodeInfo(preserveFavorites = true)
        awaitMatchesFullQuery(latest)
    }

    @Test
    fun unchangedRowsKeepTheirInstance() = runTest {
        seedMesh()
        val latest = collectLatest(dao.observeNodeDBbyNum())
        val before = awaitMatchesFullQuery(latest)

        dao.upsert(meshNode(3).copy(lastHeard = BASE_TIME + 10_000))
        val after = awaitMatchesFullQuery(latest)

        for ((num, row) in after) {
            if (num != 3) assertSame(before.getValue(num), row, "node $num did not change and must not be re-read")
        }
    }

    @Test
    fun theFingerprintCoversEveryStoredColumn() = runTest {
        seedMesh()
        dao.upsert(MetadataEntity(NODE, DeviceMetadata.Builder().also { it.firmware_version = "2.8.0" }.build()))

        for ((table, column, type) in columnsOf("nodes") + columnsOf("metadata")) {
            if (column == "num") continue
            val before = fingerprintOf(NODE)
            val changed =
                when (type.uppercase()) {
                    "INTEGER" -> "coalesce($column, 0) + 1"
                    "REAL" -> "coalesce($column, 0) + 0.5"
                    "TEXT" -> "coalesce($column, '') || 'x'"
                    "BLOB" -> "coalesce($column, x'') || x'01'"
                    else -> error("unexpected column type $type for $table.$column")
                }
            database.useWriterConnection { it.executeSQL("UPDATE $table SET $column = $changed WHERE num = $NODE") }
            assertNotEquals(before, fingerprintOf(NODE), "a change to $table.$column must change the fingerprint")
        }
    }

    @Test
    fun theFingerprintSeesTheSmallestChangeToAReal() = runTest {
        seedMesh()
        database.useWriterConnection { it.executeSQL("UPDATE nodes SET latitude = 0.1 WHERE num = $NODE") }
        val before = fingerprintOf(NODE)
        // 0.10000000000000002 is the next double above 0.1.
        database.useWriterConnection {
            it.executeSQL("UPDATE nodes SET latitude = 0.10000000000000002 WHERE num = $NODE")
        }
        assertNotEquals(before, fingerprintOf(NODE))
    }

    private suspend fun TestScope.readsPerUpsert(source: (() -> Flow<Map<Int, NodeWithRelations>>)?): Int {
        val latest = MutableStateFlow<Map<Int, NodeWithRelations>?>(null)
        val job = source?.let { backgroundScope.launch { it().collect { rows -> latest.value = rows } } }
        if (job != null) awaitMatchesFullQuery(latest)
        val readsBefore = fullNodeRowsRead.value
        repeat(UPSERTS) { i ->
            val heard = BASE_TIME + 1_000 + ++heardSeq
            dao.upsert(meshNode(1 + i).copy(lastHeard = heard))
            if (job != null) realTime { latest.first { it?.get(1 + i)?.node?.lastHeard == heard } }
        }
        val reads = fullNodeRowsRead.value - readsBefore
        job?.cancel()
        return reads
    }

    private fun TestScope.collectLatest(
        flow: Flow<Map<Int, NodeWithRelations>>,
    ): MutableStateFlow<Map<Int, NodeWithRelations>?> {
        val latest = MutableStateFlow<Map<Int, NodeWithRelations>?>(null)
        backgroundScope.launch { flow.collect { latest.value = it } }
        return latest
    }

    /** Waits until [latest] equals a fresh full-table read, in content and order, and returns it. */
    private suspend fun awaitMatchesFullQuery(
        latest: MutableStateFlow<Map<Int, NodeWithRelations>?>,
    ): Map<Int, NodeWithRelations> {
        val expected = dao.nodeDBbyNumSnapshot().toList()
        return realTime { latest.first { it?.toList() == expected } }!!
    }

    private suspend fun <T> realTime(block: suspend () -> T): T =
        withContext(Dispatchers.Default) { withTimeout(AWAIT_TIMEOUT_MS) { block() } }

    private suspend fun fingerprintOf(num: Int): String =
        dao.nodeFingerprints().first().single { it.num == num }.fingerprint

    private suspend fun columnsOf(table: String): List<Triple<String, String, String>> =
        database.useReaderConnection { connection ->
            connection.usePrepared("PRAGMA table_info($table)") { statement ->
                buildList { while (statement.step()) add(Triple(table, statement.getText(1), statement.getText(2))) }
            }
        }

    private suspend fun seedMesh() {
        dao.setMyNodeInfo(myNode(MESH_SIZE))
        dao.putAll((1..MESH_SIZE).map(::meshNode))
        (1..MESH_SIZE step 3).forEach { num ->
            dao.upsert(MetadataEntity(num, DeviceMetadata.Builder().also { it.firmware_version = "2.7.$num" }.build()))
        }
    }

    private fun myNode(num: Int) = MyNodeEntity(
        myNodeNum = num,
        model = "TBEAM",
        firmwareVersion = "2.8.0",
        couldUpdate = false,
        shouldUpdate = false,
        currentPacketId = 1L,
        messageTimeoutMsec = 300000,
        minAppVersion = 1,
        maxChannels = 8,
        hasWifi = false,
    )

    /** A node with the blobs a real mesh node carries: user with key, position, device and environment telemetry. */
    private fun meshNode(num: Int) = NodeEntity(
        num = num,
        user =
        User.Builder()
            .also {
                it.id = "!" + num.toString(16).padStart(8, '0')
                it.long_name = "Mesh node $num"
                it.short_name = "N" + num.toString().padStart(3, '0')
                it.hw_model = HardwareModel.RAK4631
                it.public_key = ByteArray(32) { b -> (b + num).toByte() }.toByteString()
            }
            .build(),
        longName = "Mesh node $num",
        shortName = "N" + num.toString().padStart(3, '0'),
        position =
        Position.Builder()
            .also {
                it.latitude_i = 377_000_000 + num
                it.longitude_i = -1_224_000_000 - num
                it.altitude = 30 + num
                it.time = BASE_TIME
            }
            .build(),
        latitude = 37.7 + num * 1e-7,
        longitude = -122.4 - num * 1e-7,
        snr = 5.25f,
        rssi = -90,
        lastHeard = BASE_TIME - num,
        deviceTelemetry =
        Telemetry.Builder()
            .also {
                it.device_metrics =
                    DeviceMetrics.Builder()
                        .also { m ->
                            m.battery_level = 80
                            m.voltage = 3.9f
                            m.channel_utilization = 4.5f
                            m.air_util_tx = 0.7f
                            m.uptime_seconds = 1000 + num
                        }
                        .build()
            }
            .build(),
        environmentTelemetry =
        Telemetry.Builder()
            .also {
                it.environment_metrics =
                    EnvironmentMetrics.Builder()
                        .also { m ->
                            m.temperature = 18.5f
                            m.relative_humidity = 60f
                            m.barometric_pressure = 1013f
                        }
                        .build()
            }
            .build(),
        hopsAway = num % 4,
        isFavorite = num % 10 == 0,
        publicKey = ByteArray(32) { b -> (b + num).toByte() }.toByteString(),
    )

    /** Counts rows stepped by statements that read whole `nodes` rows. */
    private class CountingDriver(private val delegate: SQLiteDriver, private val onFullNodeRow: () -> Unit) :
        SQLiteDriver by delegate {
        override fun open(fileName: String): SQLiteConnection =
            CountingConnection(delegate.open(fileName), onFullNodeRow)
    }

    private class CountingConnection(private val delegate: SQLiteConnection, private val onFullNodeRow: () -> Unit) :
        SQLiteConnection by delegate {
        override fun prepare(sql: String): SQLiteStatement {
            val statement = delegate.prepare(sql)
            return if (sql.trimStart().startsWith("SELECT * FROM nodes")) {
                CountingStatement(statement, onFullNodeRow)
            } else {
                statement
            }
        }
    }

    private class CountingStatement(private val delegate: SQLiteStatement, private val onRow: () -> Unit) :
        SQLiteStatement by delegate {
        override fun step(): Boolean = delegate.step().also { if (it) onRow() }
    }

    private companion object {
        const val MESH_SIZE = 150
        const val UPSERTS = 20
        const val NODE = 2
        const val BASE_TIME = 1_793_900_000
        const val AWAIT_TIMEOUT_MS = 10_000L
    }
}
