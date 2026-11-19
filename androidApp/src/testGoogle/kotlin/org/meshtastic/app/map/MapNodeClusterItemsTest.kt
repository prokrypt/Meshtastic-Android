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
package org.meshtastic.app.map

import androidx.compose.material3.Text
import androidx.compose.runtime.SideEffect
import androidx.compose.runtime.getValue
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.v2.runComposeUiTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.meshtastic.app.map.model.LiveMapNodes
import org.meshtastic.core.model.Node
import org.meshtastic.feature.map.BaseMapViewModel.MapFilterState
import org.meshtastic.feature.map.LastHeardFilter
import org.meshtastic.feature.map.MapNodePolicy
import org.meshtastic.proto.Position
import org.meshtastic.proto.User
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

// A bare Application keeps MeshUtilApplication.onCreate out of this test: its fire-and-forget
// applicationScope launches outlive the class and surface here as UncaughtExceptionsBeforeTest.
@OptIn(ExperimentalTestApi::class, ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34], application = android.app.Application::class)
class MapNodeClusterItemsTest {

    private val liveNodes = LiveMapNodes()

    @Test
    fun `a packet that moves no node hands on the same two thousand items`() {
        val nodes = testNodes(count = NODE_COUNT)
        val before = markerSet(nodes)
        assertEquals(NODE_COUNT, before.items.size)

        // Heard again: a new last-heard time, a new reading and a new name, and the repository's last-heard ordering
        // moves the node to the front.
        val heard =
            nodes[700].let { node ->
                node.copy(
                    lastHeard = NOW.toInt(),
                    snr = 7.5f,
                    user = node.user.newBuilder().also { wb -> wb.short_name = "NEW" }.build(),
                )
            }
        val after = markerSet(listOf(heard) + nodes.filterNot { it.num == heard.num }, previous = before)

        assertSame(before.items, after.items, "nothing moved, so the clustering must be handed the list it has")
        assertSame(heard, after.nodes[heard.num], "the chip still has to draw the node as it is now")
        assertNotEquals(before, after)
    }

    @Test
    fun `a node that moves gets a new item and every other item stays equal`() {
        val nodes = testNodes(count = 50)
        val before = markerSet(nodes)

        val moved =
            nodes[10].let { node ->
                node.copy(position = node.position.newBuilder().also { wb -> wb.latitude_i = 300_000_000 }.build())
            }
        val after = markerSet(nodes.map { if (it.num == moved.num) moved else it }, previous = before)

        assertNotSame(before.items, after.items)
        assertEquals(listOf(moved.num), after.items.filterNot { it in before.items }.map { it.nodeNum })
        assertEquals(listOf(moved.num), before.items.filterNot { it in after.items }.map { it.nodeNum })
    }

    @Test
    fun `what a marker is drawn with is part of its item`() {
        val node = testNodes(count = 1).single()
        val item = itemsFor(listOf(node)).single()

        assertEquals(item, itemsFor(listOf(node.copy(lastHeard = 5, snr = 1f))).single())
        assertEquals(item.hashCode(), itemsFor(listOf(node.copy(lastHeard = 5))).single().hashCode())

        // A favorite draws above its neighbors, and reduced precision draws a circle.
        val favorite = itemsFor(listOf(node.copy(isFavorite = true))).single()
        assertNotEquals(item, favorite)
        assertEquals(MapNodePolicy.PRIORITY_PROMINENT.toFloat(), favorite.zIndex)
        val imprecise =
            itemsFor(listOf(node.copy(position = node.position.newBuilder().also { it.precision_bits = 13 }.build())))
                .single()
        assertNotEquals(item, imprecise)
        assertNull(item.precisionMeters)
        assertTrue(imprecise.precisionMeters!! > 0)
    }

    @Test
    fun `the order nodes arrive in does not regroup the map`() {
        // A chain about 3 m apart: each node stacks with the next but not the one after, so which node anchors a
        // stack decides the stacks.
        val origin = testNodes(count = 1).single()
        val nodes =
            testNodes(count = 9).mapIndexed { index, node ->
                val position =
                    origin.position
                        .newBuilder()
                        .also { it.latitude_i = (origin.position.latitude_i ?: 0) + index * CHAIN_STEP }
                        .build()
                node.copy(position = position)
            }
        val forward = markerSet(nodes)
        val reversed = markerSet(nodes.reversed())

        assertEquals(forward.items, reversed.items)
        assertTrue(forward.items.size in 2 until nodes.size)
    }

    @Test
    fun `the time filters are applied as of the clock they are given`() {
        val node = testNodes(count = 1).single().copy(lastHeard = (NOW - 3_000).toInt())
        val filter = FILTER.copy(lastHeardFilter = LastHeardFilter.OneHour)

        assertEquals(1, markerSet(listOf(node), filter = filter, now = NOW).items.size)
        assertTrue(markerSet(listOf(node), filter = filter, now = NOW + 700).items.isEmpty())
    }

    @Test
    fun `nodes at one spot become one item that carries every member`() {
        val spot = testNodes(count = 1).single().position
        val nodes = testNodes(count = 4).mapIndexed { i, node -> if (i < 3) node.copy(position = spot) else node }

        val items = itemsFor(nodes, myNodeNum = 2)

        assertEquals(2, items.size)
        val stack = items.first()
        assertEquals(2, stack.nodeNum)
        assertEquals(listOf(1, 2, 3), stack.stackMemberNums)
        assertTrue(items.last().stackMemberNums.isEmpty())
    }

    @Test
    fun `nodes that are merely near each other keep an item each`() {
        // 11 m apart. Whether their chips overlap depends on the zoom, which is the clustering's to judge.
        val items = itemsFor(testNodes(count = 12))

        assertEquals(12, items.size)
        assertTrue(items.all { it.stackMemberNums.isEmpty() })
    }

    @Test
    fun `a marker's title is read from the node as it is now`() {
        val node = testNodes(count = 1).single()
        val item = itemsFor(listOf(node)).single()
        assertNull(item.title, "nothing is known about the node until it is published")

        liveNodes.setRelativeTimeTexts(unknown = "unknown", now = "now")
        liveNodes.update(mapOf(node.num to node))
        assertTrue(item.title!!.startsWith("N0 "))
        assertEquals("Node 1", item.snippet)

        val renamed = node.copy(user = node.user.newBuilder().also { wb -> wb.short_name = "NEW" }.build())
        liveNodes.update(mapOf(node.num to renamed))
        assertTrue(item.title!!.startsWith("NEW "), "the same item, with no clustering pass in between")

        val untimed = renamed.copy(position = renamed.position.newBuilder().also { wb -> wb.time = 0 }.build())
        liveNodes.update(mapOf(node.num to untimed))
        assertEquals("NEW unknown", item.title)
    }

    @Test
    fun `a packet redraws the chip of the node it came from and no other`() = runComposeUiTest {
        val nodes = testNodes(count = 2)
        liveNodes.update(nodes.associateBy { it.num })
        val draws = IntArray(2)

        setContent {
            nodes.forEachIndexed { index, node ->
                NodeText(liveNodes, node.num, onDraw = { draws[index]++ })
            }
        }
        waitForIdle()
        assertEquals(listOf(1, 1), draws.toList())

        val heard = nodes[0].copy(lastHeard = NOW.toInt())
        runOnIdle { liveNodes.update(mapOf(heard.num to heard, nodes[1].num to nodes[1])) }
        waitForIdle()
        assertEquals(listOf(2, 1), draws.toList())

        // The same nodes again, as a minute tick publishes them: nothing to redraw.
        runOnIdle { liveNodes.update(mapOf(heard.num to heard, nodes[1].num to nodes[1])) }
        waitForIdle()
        assertEquals(listOf(2, 1), draws.toList())
    }

    @Test
    fun `a node that leaves the map keeps its last value for the marker still drawing it`() {
        val nodes = testNodes(count = 2)
        liveNodes.update(nodes.associateBy { it.num })
        val state = liveNodes.state(nodes[1].num)

        liveNodes.update(mapOf(nodes[0].num to nodes[0]))
        assertSame(nodes[1], state.value)

        val back = nodes[1].copy(lastHeard = NOW.toInt())
        liveNodes.update(mapOf(nodes[0].num to nodes[0], back.num to back))
        assertSame(back, state.value, "the state a chip already reads is the one that is updated")
        assertFalse(liveNodes.state(99).value != null)
    }

    @Test
    fun `relative time bucket advances at the next minute boundary`() = runTest {
        val startSeconds = FIXED_TIME_BUCKET * 60 + 30
        val observedBuckets = mutableListOf<Long>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            relativeTimeBuckets { startSeconds + testScheduler.currentTime / 1_000 }.take(2).toList(observedBuckets)
        }

        runCurrent()
        assertEquals(listOf(FIXED_TIME_BUCKET), observedBuckets)

        advanceTimeBy(29_999)
        runCurrent()
        assertEquals(listOf(FIXED_TIME_BUCKET), observedBuckets)

        advanceTimeBy(1)
        runCurrent()
        assertEquals(listOf(FIXED_TIME_BUCKET, FIXED_TIME_BUCKET + 1), observedBuckets)
    }

    @androidx.compose.runtime.Composable
    private fun NodeText(liveNodes: LiveMapNodes, nodeNum: Int, onDraw: () -> Unit) {
        val node by liveNodes.state(nodeNum)
        Text(node?.user?.short_name.orEmpty())
        SideEffect(onDraw)
    }

    private fun markerSet(
        nodes: List<Node>,
        filter: MapFilterState = FILTER,
        now: Long = NOW,
        previous: NodeMarkerSet? = null,
    ) = buildNodeMarkerSet(
        nodes = nodes,
        filterState = filter,
        nowSeconds = now,
        ourNodeNum = null,
        myNodeNum = null,
        liveNodes = liveNodes,
        previous = previous,
    )

    private fun itemsFor(nodes: List<Node>, myNodeNum: Int? = null) = buildNodeClusterItems(nodes, myNodeNum, liveNodes)

    private companion object {
        const val NODE_COUNT = 2_000
        const val FIXED_TIME_BUCKET = 123L
        const val NOW = 1_700_100_000L

        // In 1e-7 degrees of latitude: about 3.3 m.
        const val CHAIN_STEP = 300

        val FILTER =
            MapFilterState(
                onlyFavorites = false,
                showWaypoints = true,
                showPrecisionCircle = true,
                lastHeardFilter = LastHeardFilter.Any,
                lastHeardTrackFilter = LastHeardFilter.Any,
            )
    }
}

internal fun testNodes(count: Int): List<Node> = List(count) { index ->
    Node(
        num = index + 1,
        user =
        User.Builder()
            .also { wb ->
                wb.id = "!${index + 1}"
                wb.long_name = "Node ${index + 1}"
                wb.short_name = "N$index"
            }
            .build(),
        position =
        Position.Builder()
            .also { wb ->
                // About 11 m apart, so no two fall within the 5 m that stacks nodes.
                wb.latitude_i = 210_000_000 + index * 1_000
                wb.longitude_i = -1_570_000_000 + index * 1_000
                wb.time = 1_700_000_000
            }
            .build(),
    )
}
