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
import org.meshtastic.core.database.BusyTimeoutSQLiteDriver
import org.meshtastic.core.database.MeshtasticDatabase
import org.meshtastic.core.database.entity.MeshLog
import org.meshtastic.core.database.getInMemoryDatabaseBuilder
import org.meshtastic.proto.PortNum
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [observeLogsFrom] and [observeLogsOnPort] must emit what the Room queries they replace return, while skipping the
 * full read when only another node's (or port's) logs changed. Full log rows are counted at the SQLite driver.
 */
class LogFlowsTest {
    private lateinit var database: MeshtasticDatabase
    private lateinit var dao: MeshLogDao
    private val fullLogRowsRead = atomic(0)
    private var seq = 0

    @BeforeTest
    fun setUp() {
        database =
            getInMemoryDatabaseBuilder()
                .setDriver(
                    CountingDriver(BusyTimeoutSQLiteDriver(BundledSQLiteDriver())) {
                        fullLogRowsRead.incrementAndGet()
                    },
                )
                .build()
        dao = database.meshLogDao()
    }

    @AfterTest
    fun tearDown() {
        database.close()
    }

    @Test
    fun logsFromAnotherNodeDoNotReReadThisNodesRows() = runTest {
        repeat(HISTORY) { dao.insert(log(from = NODE, port = TELEMETRY)) }
        val latest = collectLatest(dao.observeLogsFrom(NODE, TELEMETRY, MAX))
        awaitMatches(latest) { dao.getLogsSnapshot(NODE, TELEMETRY, MAX) }

        val before = fullLogRowsRead.value
        repeat(OTHER_PACKETS) { dao.insert(log(from = OTHER, port = TELEMETRY)) }
        // A write from this node is the signal that every earlier write has been observed.
        val own = log(from = NODE, port = TELEMETRY)
        dao.insert(own)
        realTime { latest.first { it?.firstOrNull()?.uuid == own.uuid } }
        val reads = fullLogRowsRead.value - before

        println("Full log rows read for $OTHER_PACKETS other-node packets and 1 own packet: $reads")
        // The one re-read for this node's own packet, plus the snapshot awaitMatches reads.
        assertEquals(HISTORY + 1, reads)
    }

    @Test
    fun emissionsMatchTheRoomQueryAcrossInsertsAndDeletes() = runTest {
        repeat(HISTORY) { i -> dao.insert(log(from = NODE, port = if (i % 2 == 0) TELEMETRY else POSITION)) }
        for (port in listOf(TELEMETRY, POSITION, -1)) {
            val latest = collectLatest(dao.observeLogsFrom(NODE, port, MAX))
            val expected = suspend { dao.getLogsFrom(NODE, port, MAX).first() }
            awaitMatches(latest, expected)

            dao.insert(log(from = NODE, port = TELEMETRY))
            awaitMatches(latest, expected)

            // Deleting the newest row and inserting another lets SQLite reuse its rowid; the stamp still moves.
            dao.deleteLog(dao.getLogsSnapshot(NODE, -1, 1).single().uuid)
            dao.insert(log(from = NODE, port = POSITION))
            awaitMatches(latest, expected)

            dao.deleteLogs(NODE, POSITION)
            awaitMatches(latest, expected)
        }
    }

    @Test
    fun logsOnPortFollowTheRoomQuery() = runTest {
        repeat(HISTORY) { i -> dao.insert(log(from = if (i % 3 == 0) 0 else NODE + i, port = TRACEROUTE)) }
        val latest = collectLatest(dao.observeLogsOnPort(TRACEROUTE, 0, MAX))
        val expected = suspend { dao.getLogsOnPort(TRACEROUTE, 0, MAX).first() }
        awaitMatches(latest, expected)

        dao.insert(log(from = OTHER, port = TRACEROUTE))
        awaitMatches(latest, expected)

        dao.deleteLogs(OTHER, TRACEROUTE)
        awaitMatches(latest, expected)
    }

    private fun log(from: Int, port: Int): MeshLog {
        val n = ++seq
        return MeshLog(
            uuid = "log-$n",
            message_type = "Packet",
            received_date = BASE_TIME + n,
            raw_message = "",
            fromNum = from,
            portNum = port,
        )
    }

    private fun TestScope.collectLatest(flow: Flow<List<MeshLog>>): MutableStateFlow<List<MeshLog>?> {
        val latest = MutableStateFlow<List<MeshLog>?>(null)
        backgroundScope.launch { flow.collect { latest.value = it } }
        return latest
    }

    private suspend fun awaitMatches(latest: MutableStateFlow<List<MeshLog>?>, expected: suspend () -> List<MeshLog>) {
        val want = expected()
        realTime { latest.first { it == want } }
    }

    private suspend fun <T> realTime(block: suspend () -> T): T =
        withContext(Dispatchers.Default) { withTimeout(AWAIT_TIMEOUT_MS) { block() } }

    /** Counts rows stepped by statements that read whole `log` rows. */
    private class CountingDriver(private val delegate: SQLiteDriver, private val onFullRow: () -> Unit) :
        SQLiteDriver by delegate {
        override fun open(fileName: String): SQLiteConnection = CountingConnection(delegate.open(fileName), onFullRow)
    }

    private class CountingConnection(private val delegate: SQLiteConnection, private val onFullRow: () -> Unit) :
        SQLiteConnection by delegate {
        override fun prepare(sql: String): SQLiteStatement {
            val statement = delegate.prepare(sql)
            return if (sql.trimStart().startsWith("SELECT * FROM log")) {
                CountingStatement(statement, onFullRow)
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
        const val NODE = 7
        const val OTHER = 99
        const val HISTORY = 40
        const val OTHER_PACKETS = 25
        const val MAX = 5000
        const val BASE_TIME = 1_793_900_000_000L
        const val AWAIT_TIMEOUT_MS = 10_000L
        val TELEMETRY = PortNum.TELEMETRY_APP.value
        val POSITION = PortNum.POSITION_APP.value
        val TRACEROUTE = PortNum.TRACEROUTE_APP.value
    }
}
