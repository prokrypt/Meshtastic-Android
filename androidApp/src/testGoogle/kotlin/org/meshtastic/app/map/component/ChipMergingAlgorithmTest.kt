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
package org.meshtastic.app.map.component

import com.google.android.gms.maps.model.LatLng
import com.google.maps.android.clustering.Cluster
import com.google.maps.android.clustering.algo.NonHierarchicalDistanceBasedAlgorithm
import com.google.maps.android.clustering.algo.ScreenBasedAlgorithmAdapter
import com.google.maps.android.clustering.algo.StaticCluster
import org.junit.Test
import org.meshtastic.app.map.model.LiveMapNodes
import org.meshtastic.app.map.model.NodeClusterItem
import org.meshtastic.feature.map.MapNodePolicy
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Which chips share a marker once the clustering has decided its bubbles.
 *
 * Distances are in dp on screen at [ZOOM], east of one point, where Web Mercator makes them exact.
 */
class ChipMergingAlgorithmTest {

    private val liveNodes = LiveMapNodes()

    @Test
    fun `chips the clustering left beside a bubble merge where they overlap`() {
        // What the map showed before: a bubble, and next to it chips lying on each other because the group they
        // belonged to had been held back for a bubble that then took only some of it.
        val bubble = clusterOf((1..12).map { item(it, eastDp = it.toDouble()) })
        val leftOver = clusterOf(listOf(item(20, eastDp = 150.0), item(21, eastDp = 153.0)))
        val alone = clusterOf(listOf(item(22, eastDp = 160.0)))

        val merged = mergeLooseChips(setOf(bubble, leftOver, alone), ZOOM)

        assertEquals(2, merged.size)
        assertTrue(merged.any { it === bubble }, "a bubble is handed back as it was")
        val chip = merged.single { it !== bubble }.items.single()
        assertEquals(20, chip.nodeNum)
        assertEquals(listOf(20, 21, 22), chip.stackMemberNums)
    }

    @Test
    fun `ten nodes on one spot and the chips around them share one chip`() {
        // Ten nodes at one spot are one item, so the clustering sees four items here and draws no bubble. Counted as
        // thirteen nodes they used to be left unmerged for it all the same.
        val stack = item(1, eastDp = 0.0, members = (1..10).toList())
        val around = listOf(item(11, eastDp = 8.0), item(12, eastDp = 14.0), item(13, eastDp = 20.0))
        val algorithm = ChipMergingAlgorithm(ScreenBasedAlgorithmAdapter(NonHierarchicalDistanceBasedAlgorithm()))
        algorithm.addItems(listOf(stack) + around)

        val chip = algorithm.getClusters(ZOOM.toFloat()).single().items.single()

        assertEquals(1, chip.nodeNum)
        assertEquals((1..13).toList(), chip.stackMemberNums)
    }

    @Test
    fun `chips merge only within the merge distance at the zoom they are drawn at`() {
        val near = item(1, eastDp = 0.0)
        val far = item(2, eastDp = MERGE_DISTANCE_DP + 12.0)
        val clusters = setOf(clusterOf(listOf(near)), clusterOf(listOf(far)))

        val apart = mergeLooseChips(clusters, ZOOM)
        assertEquals(setOf(near, far), apart.flatMapTo(HashSet()) { it.items })
        assertTrue(apart.flatMap { it.items }.all { it === near || it === far }, "an unmerged chip is its own item")

        // One level out everything on screen is half as far apart.
        val together = mergeLooseChips(clusters, ZOOM - 1)
        assertEquals(listOf(1, 2), together.single().items.single().stackMemberNums)
    }

    @Test
    fun `a chip joins the nearest chip that is close enough`() {
        val left = item(1, eastDp = 0.0)
        val right = item(2, eastDp = 40.0)
        val between = item(3, eastDp = 25.0)

        val merged = mergeLooseChips(setOf(clusterOf(listOf(left, right, between))), ZOOM)

        val chips = merged.flatMap { it.items }.sortedBy { it.nodeNum }
        assertEquals(listOf(1, 2), chips.map { it.nodeNum })
        assertTrue(chips[0].stackMemberNums.isEmpty())
        assertEquals(listOf(2, 3), chips[1].stackMemberNums)
    }

    @Test
    fun `enough loose chips at one place are a bubble`() {
        // The clustering can split a tight group between two of its own, each too small to bubble.
        val one = (1..5).map { item(it, eastDp = it.toDouble()) }
        val other = (6..10).map { item(it, eastDp = it.toDouble()) }

        val bubble = mergeLooseChips(setOf(clusterOf(one), clusterOf(other)), ZOOM).single()

        assertEquals(MIN_CLUSTER_SIZE, bubble.size)
        assertEquals((one + other).toSet(), bubble.items.toSet())
    }

    @Test
    fun `this node and favorites keep their chip when others merge into it`() {
        val ordinary = item(2, eastDp = 0.0)
        val favorite = item(5, eastDp = 10.0, priority = MapNodePolicy.PRIORITY_PROMINENT)

        val chip = mergeLooseChips(setOf(clusterOf(listOf(ordinary, favorite))), ZOOM).single().items.single()

        assertEquals(5, chip.nodeNum)
        assertEquals(favorite.position, chip.position)
        assertEquals(favorite.zIndex, chip.zIndex)
        assertEquals(listOf(5, 2), chip.stackMemberNums)
    }

    @Test
    fun `the same chips merge the same way however the clustering hands them over`() {
        val items = (1..9).map { item(it, eastDp = it * 9.0) }

        val oneWay = mergeLooseChips(setOf(clusterOf(items.take(4)), clusterOf(items.drop(4))), ZOOM)
        val another = mergeLooseChips(items.reversed().mapTo(HashSet()) { clusterOf(listOf(it)) }, ZOOM)

        // Equal clusters are what lets the renderer keep the markers it has already drawn.
        assertEquals(oneWay, another)
        assertTrue(oneWay.size in 2 until items.size)
    }

    @Test
    fun `a real bubble survives and the chips outside it merge`() {
        val crowd = (1..12).map { item(it, eastDp = it * 3.0) }
        val pair = listOf(item(20, eastDp = 400.0), item(21, eastDp = 410.0))
        val algorithm = ChipMergingAlgorithm(ScreenBasedAlgorithmAdapter(NonHierarchicalDistanceBasedAlgorithm()))
        algorithm.addItems(crowd + pair)

        val clusters = algorithm.getClusters(ZOOM.toFloat())

        assertEquals(2, clusters.size)
        assertEquals(crowd.toSet(), clusters.single { it.size >= MIN_CLUSTER_SIZE }.items.toSet())
        assertEquals(listOf(20, 21), clusters.single { it.size == 1 }.items.single().stackMemberNums)
    }

    private fun clusterOf(items: List<NodeClusterItem>): Cluster<NodeClusterItem> =
        StaticCluster<NodeClusterItem>(items.first().position).apply { items.forEach(::add) }

    private fun item(
        nodeNum: Int,
        eastDp: Double,
        priority: Int = MapNodePolicy.PRIORITY_ORDINARY,
        members: List<Int> = emptyList(),
    ) = NodeClusterItem(
        nodeNum = nodeNum,
        nodePosition = LatLng(ORIGIN.latitude, ORIGIN.longitude + eastDp * DEGREES_PER_DP),
        priority = priority,
        precisionMeters = null,
        stackMemberNums = members,
        liveNodes = liveNodes,
    )

    private companion object {
        const val ZOOM = 14
        val ORIGIN = LatLng(37.8, -122.2)

        // The world is 256 dp wide at zoom 0 and twice that with each level.
        const val DEGREES_PER_DP = 360.0 / (256 * (1 shl ZOOM))
    }
}
