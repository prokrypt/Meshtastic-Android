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
package org.meshtastic.core.data.repository

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import dev.mokkery.MockMode
import dev.mokkery.answering.returns
import dev.mokkery.answering.throws
import dev.mokkery.every
import dev.mokkery.everySuspend
import dev.mokkery.mock
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import org.meshtastic.core.common.util.Exceptions
import org.meshtastic.core.common.util.nowSeconds
import org.meshtastic.core.data.datasource.NodeInfoReadDataSource
import org.meshtastic.core.data.datasource.NodeInfoWriteDataSource
import org.meshtastic.core.database.entity.MyNodeEntity
import org.meshtastic.core.database.entity.NodeEntity
import org.meshtastic.core.database.entity.NodeWithRelations
import org.meshtastic.core.di.CoroutineDispatchers
import org.meshtastic.core.model.MeshLog
import org.meshtastic.core.testing.FakeLocalStatsDataSource
import org.meshtastic.proto.LocalStats
import org.meshtastic.proto.User
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

abstract class CommonNodeRepositoryTest {

    protected lateinit var lifecycleOwner: LifecycleOwner
    protected lateinit var readDataSource: NodeInfoReadDataSource
    protected lateinit var writeDataSource: NodeInfoWriteDataSource
    protected lateinit var localStatsDataSource: FakeLocalStatsDataSource
    private val testDispatcher = UnconfinedTestDispatcher()
    private val dispatchers = CoroutineDispatchers(main = testDispatcher, io = testDispatcher, default = testDispatcher)

    private val myNodeInfoFlow = MutableStateFlow<MyNodeEntity?>(null)

    protected lateinit var repository: NodeRepositoryImpl

    @BeforeTest
    fun setupRepo() {
        Dispatchers.setMain(testDispatcher)
        lifecycleOwner =
            object : LifecycleOwner {
                override val lifecycle = LifecycleRegistry(this)
            }
        (lifecycleOwner.lifecycle as LifecycleRegistry).handleLifecycleEvent(Lifecycle.Event.ON_RESUME)

        readDataSource = mock(MockMode.autofill)
        writeDataSource = mock(MockMode.autofill)
        localStatsDataSource = FakeLocalStatsDataSource()

        every { readDataSource.myNodeInfoFlow() } returns myNodeInfoFlow
        every { readDataSource.nodeDBbyNumFlow() } returns MutableStateFlow<Map<Int, NodeWithRelations>>(emptyMap())

        repository =
            NodeRepositoryImpl(
                lifecycleOwner.lifecycle,
                readDataSource,
                writeDataSource,
                dispatchers,
                localStatsDataSource,
            )
    }

    @AfterTest
    fun tearDown() {
        // Essential to stop background jobs in NodeRepositoryImpl
        (lifecycleOwner.lifecycle as LifecycleRegistry).handleLifecycleEvent(Lifecycle.Event.ON_DESTROY)
        Dispatchers.resetMain()
    }

    private fun createMyNodeEntity(nodeNum: Int) = MyNodeEntity(
        myNodeNum = nodeNum,
        model = "model",
        firmwareVersion = "1.0",
        couldUpdate = false,
        shouldUpdate = false,
        currentPacketId = 0L,
        messageTimeoutMsec = 0,
        minAppVersion = 0,
        maxChannels = 0,
        hasWifi = false,
    )

    private fun localStats(uptime: Int) = LocalStats.Builder().also { it.uptime_seconds = uptime }.build()

    @Test
    fun `local stats reach readers at once and are saved at most once per interval`() = runTest(testDispatcher) {
        val saved = localStatsDataSource.localStatsFlow

        repository.updateLocalStats(localStats(1))
        assertEquals(localStats(1), repository.localStats.value)
        assertEquals(localStats(1), saved.value, "the first report is saved at once")

        repository.updateLocalStats(localStats(2))
        repository.updateLocalStats(localStats(3))
        assertEquals(localStats(3), repository.localStats.value, "readers see every report at once")
        assertEquals(localStats(1), saved.value, "later reports wait for the interval")

        advanceTimeBy(10.minutes)
        runCurrent()
        assertEquals(localStats(3), saved.value, "the newest report is saved when the interval ends")

        repository.updateLocalStats(localStats(4))
        repository.updateLocalStats(LocalStats.Builder().build())
        assertEquals(LocalStats.Builder().build(), saved.value, "a clear is saved at once")
        assertEquals(LocalStats.Builder().build(), repository.localStats.value)
    }

    @Test
    fun `effectiveLogNodeId maps local node number to NODE_NUM_LOCAL`() = runTest(testDispatcher) {
        val myNodeNum = 12345
        myNodeInfoFlow.value = createMyNodeEntity(myNodeNum)

        val result = repository.effectiveLogNodeId(myNodeNum).filter { it == MeshLog.NODE_NUM_LOCAL }.first()

        assertEquals(MeshLog.NODE_NUM_LOCAL, result)
    }

    @Test
    fun `effectiveLogNodeId preserves remote node numbers`() = runTest(testDispatcher) {
        val myNodeNum = 12345
        val remoteNodeNum = 67890
        myNodeInfoFlow.value = createMyNodeEntity(myNodeNum)

        val result = repository.effectiveLogNodeId(remoteNodeNum).first()

        assertEquals(remoteNodeNum, result)
    }

    @Test
    fun `a failing startup name backfill is reported instead of escaping the process scope`() =
        runTest(testDispatcher) {
            val failure = IllegalStateException("withDb callback did not finish")
            val reported = mutableListOf<Throwable>()
            val previousReporter = Exceptions.reporter
            Exceptions.reporter = { throwable, _, _ -> reported += throwable }
            try {
                val failingWriteDataSource = mock<NodeInfoWriteDataSource>(MockMode.autofill)
                everySuspend { failingWriteDataSource.backfillDenormalizedNames() } throws failure

                NodeRepositoryImpl(
                    lifecycleOwner.lifecycle,
                    readDataSource,
                    failingWriteDataSource,
                    dispatchers,
                    localStatsDataSource,
                )

                // Coroutine stack-trace recovery may report a copy, so compare what was reported, not identity.
                assertEquals(listOf(failure.message), reported.map { it.message })
            } finally {
                Exceptions.reporter = previousReporter
            }
        }

    @Test
    fun `nodeDBbyNum maps only rows that changed since the last emission`() = runTest(testDispatcher) {
        fun row(num: Int, name: String) = NodeWithRelations(
            NodeEntity(
                num = num,
                user =
                User.Builder()
                    .also {
                        it.id = "!$num"
                        it.long_name = name
                    }
                    .build(),
            ),
            metadata = null,
        )
        val unchanged = row(1, "one")
        val rows = MutableStateFlow(mapOf(1 to unchanged, 2 to row(2, "two")))
        every { readDataSource.nodeDBbyNumFlow() } returns rows
        val repo =
            NodeRepositoryImpl(
                lifecycleOwner.lifecycle,
                readDataSource,
                writeDataSource,
                dispatchers,
                localStatsDataSource,
            )
        val before = repo.nodeDBbyNum.filter { it.size == 2 }.first()

        rows.value = mapOf(2 to row(2, "two, renamed"), 1 to unchanged)
        val after = repo.nodeDBbyNum.filter { it[2]?.user?.long_name == "two, renamed" }.first()

        assertSame(before.getValue(1), after.getValue(1))
        assertNotSame(before.getValue(2), after.getValue(2))
        assertEquals(listOf(2, 1), after.keys.toList(), "order follows the database")
        assertEquals(unchanged.toModel(), after.getValue(1))
    }

    private fun row(num: Int, lastHeard: Int, snr: Float = 0f, longName: String = "Node $num") = NodeWithRelations(
        NodeEntity(
            num = num,
            user =
            User.Builder()
                .also {
                    it.id = "!$num"
                    it.long_name = longName
                }
                .build(),
            lastHeard = lastHeard,
            snr = snr,
        ),
        metadata = null,
    )

    private fun repositoryOver(rows: MutableStateFlow<Map<Int, NodeWithRelations>>): NodeRepositoryImpl {
        every { readDataSource.nodeDBbyNumFlow() } returns rows
        return NodeRepositoryImpl(
            lifecycleOwner.lifecycle,
            readDataSource,
            writeDataSource,
            dispatchers,
            localStatsDataSource,
        )
    }

    @Test
    fun `unsaved readings show over their row until the row catches up`() = runTest(testDispatcher) {
        val rows = MutableStateFlow(mapOf(1 to row(1, 100), 2 to row(2, 100)))
        val repo = repositoryOver(rows)
        val stored = repo.nodeDBbyNum.filter { it.size == 2 }.first().getValue(1)

        val unsaved =
            stored.copy(
                lastHeard = 160,
                snr = 5f,
                user = stored.user.newBuilder().also { it.long_name = "Renamed" }.build(),
            )
        repo.showUnsavedNode(unsaved)
        val shown = repo.nodeDBbyNum.filter { it[1]?.lastHeard == 160 }.first()
        assertEquals(5f, shown[1]?.snr, "every held reading is shown")
        assertEquals("Node 1", shown[1]?.user?.long_name, "only readings are laid over the row")
        assertEquals(100, shown[2]?.lastHeard, "other nodes keep their row")

        repo.forgetUnsavedNode(unsaved)
        assertEquals(160, repo.nodeDBbyNum.value[1]?.lastHeard, "still shown while the row is older")

        rows.value = mapOf(1 to row(1, 160, snr = 5f), 2 to row(2, 100))
        rows.value = mapOf(1 to row(1, 200), 2 to row(2, 100))
        val written = repo.nodeDBbyNum.filter { it[1]?.lastHeard == 200 }.first()
        assertEquals(0f, written[1]?.snr, "once the row caught up, newer rows show as stored")
    }

    @Test
    fun `only a readings change of a stored node that is online may wait`() = runTest(testDispatcher) {
        val now = nowSeconds.toInt()
        val rows = MutableStateFlow(mapOf(1 to row(1, now), 2 to row(2, 100)))
        val repo = repositoryOver(rows)
        val nodes = repo.nodeDBbyNum.filter { it.size == 2 }.first()
        val online = nodes.getValue(1)

        assertTrue(repo.canDeferWrite(online.copy(lastHeard = now + 30, snr = 4f)), "new readings may wait")
        assertFalse(
            repo.canDeferWrite(
                online.copy(user = online.user.newBuilder().also { it.long_name = "Renamed" }.build()),
            ),
            "a name change is written at once",
        )
        assertFalse(repo.canDeferWrite(online.copy(hopsAway = 0)), "a change of direct reach is written at once")
        assertFalse(
            repo.canDeferWrite(nodes.getValue(2).copy(lastHeard = now)),
            "a node coming back online is written at once",
        )
        assertFalse(repo.canDeferWrite(online.copy(num = 3)), "a node with no row is written at once")
    }
}
