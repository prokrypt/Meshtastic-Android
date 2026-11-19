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

import com.google.maps.android.clustering.Cluster
import com.google.maps.android.clustering.algo.ScreenBasedAlgorithm
import com.google.maps.android.clustering.algo.StaticCluster
import org.meshtastic.app.map.model.NodeClusterItem
import kotlin.math.PI
import kotlin.math.floor
import kotlin.math.hypot
import kotlin.math.ln
import kotlin.math.sin

/**
 * Chips closer than this on screen merge into one chip with a count badge: one chip's minimum height
 * ([org.meshtastic.core.ui.component.NodeChip]), so only chips that would sit almost on top of each other merge.
 * Measured at the whole zoom level, as the clustering measures its own distance.
 */
const val MERGE_DISTANCE_DP = 28

/**
 * The map's clustering, with the chips it leaves outside its bubbles merged where they overlap.
 *
 * [delegate] decides the bubbles. Whatever it does not gather into one it would draw as a chip apiece, however close
 * they sit, so those are merged here, after the bubbles are known. Merging before the clustering, and stepping aside
 * for any group big enough to bubble, left chips piled on each other whenever the clustering then drew no bubble for
 * them: beside a bubble it had already made of their neighbours, or where ten nodes were one stack and so one item.
 */
internal class ChipMergingAlgorithm(val delegate: ScreenBasedAlgorithm<NodeClusterItem>) :
    ScreenBasedAlgorithm<NodeClusterItem> by delegate {
    override fun getClusters(zoom: Float): Set<Cluster<NodeClusterItem>> =
        mergeLooseChips(delegate.getClusters(zoom), zoom.toInt())
}

/**
 * [clusters] with every item outside a bubble merged with the items within [mergeDistanceDp] of it at [zoom].
 *
 * A cluster of [minBubbleSize] or more is a bubble and is handed back as it is. The rest are taken apart and grouped
 * again: each group is anchored on its most prominent item, lowest node number first, and takes the items nearer to
 * that anchor than [mergeDistanceDp]. A group comes back as one item that lists every node in it, drawn where its
 * anchor is, or as a bubble of its own when it holds [minBubbleSize] items.
 *
 * The same items in give equal clusters out, whatever order they arrive in, so the renderer keeps its markers.
 */
internal fun mergeLooseChips(
    clusters: Set<Cluster<NodeClusterItem>>,
    zoom: Int,
    minBubbleSize: Int = MIN_CLUSTER_SIZE,
    mergeDistanceDp: Double = MERGE_DISTANCE_DP.toDouble(),
): Set<Cluster<NodeClusterItem>> {
    val result = HashSet<Cluster<NodeClusterItem>>(clusters.size)
    val loose = ArrayList<NodeClusterItem>()
    for (cluster in clusters) {
        if (cluster.size >= minBubbleSize) result += cluster else loose += cluster.items
    }
    loose.sortWith(ANCHOR_ORDER)

    val dpPerWorld = TILE_SIZE_DP * (1L shl zoom.coerceIn(0, MAX_ZOOM))
    val groups = ChipGroups(mergeDistanceDp)
    for (item in loose) {
        groups.add(
            item = item,
            x = worldX(item.position.longitude) * dpPerWorld,
            y = worldY(item.position.latitude) * dpPerWorld,
        )
    }
    groups.all.mapTo(result) { it.toCluster(minBubbleSize) }
    return result
}

/** The groups made so far, findable by where their anchors are. */
private class ChipGroups(private val mergeDistanceDp: Double) {
    val all = ArrayList<ChipGroup>()

    // Anchors by grid cell one merge distance wide, so an item is measured against the anchors around it only.
    private val cells = HashMap<Long, MutableList<ChipGroup>>()

    /** Puts [item], at [x], [y] in dp, in the group whose anchor is nearest and close enough, or starts one on it. */
    fun add(item: NodeClusterItem, x: Double, y: Double) {
        val cellX = floor(x / mergeDistanceDp).toLong()
        val cellY = floor(y / mergeDistanceDp).toLong()
        val group = nearestWithinReach(x, y, cellX, cellY)
        if (group != null) {
            group.members += item
        } else {
            val anchored = ChipGroup(x, y, item)
            all += anchored
            cells.getOrPut(cellKey(cellX, cellY)) { ArrayList(1) } += anchored
        }
    }

    private fun nearestWithinReach(x: Double, y: Double, cellX: Long, cellY: Long): ChipGroup? {
        var nearest: ChipGroup? = null
        var nearestDistance = mergeDistanceDp
        for (dx in -1L..1L) {
            for (dy in -1L..1L) {
                for (group in cells[cellKey(cellX + dx, cellY + dy)].orEmpty()) {
                    val distance = hypot(group.x - x, group.y - y)
                    if (distance < nearestDistance) {
                        nearest = group
                        nearestDistance = distance
                    }
                }
            }
        }
        return nearest
    }
}

/** Items within the merge distance of [x], [y], which is where the first of them, its anchor, sits. */
private class ChipGroup(val x: Double, val y: Double, anchor: NodeClusterItem) {
    val members = arrayListOf(anchor)

    fun toCluster(minBubbleSize: Int): Cluster<NodeClusterItem> {
        val anchor = members.first()
        val cluster = StaticCluster<NodeClusterItem>(anchor.position)
        when {
            members.size == 1 -> cluster.add(anchor)

            // Bubbles win: enough chips at one place to bubble are a bubble, not a chip with a large badge.
            members.size >= minBubbleSize -> members.forEach(cluster::add)

            else -> cluster.add(anchor.mergedWith(members))
        }
        return cluster
    }
}

// This node and favorites first, so their chip is the one drawn; then by number, which no packet changes.
private val ANCHOR_ORDER = compareByDescending<NodeClusterItem> { it.priority }.thenBy { it.nodeNum }

private const val TILE_SIZE_DP = 256.0

// Past Google's deepest zoom, and small enough that the world's width in dp stays far inside a Long.
private const val MAX_ZOOM = 30

// Where Web Mercator stops; nearer a pole than this its y runs off to infinity.
private const val MAX_MERCATOR_LATITUDE = 85.051_128_78
private const val HALF = 0.5
private const val FULL_TURN_DEGREES = 360.0
private const val RADIANS_PER_DEGREE = PI / 180
private const val TWO_TURNS_RADIANS = 4 * PI
private const val CELL_BITS = 32
private const val CELL_MASK = 0xFFFF_FFFFL

private fun cellKey(x: Long, y: Long): Long = (x shl CELL_BITS) or (y and CELL_MASK)

/** Web Mercator, as a fraction of the world's width: 0 at the antimeridian's west side, 1 at its east. */
private fun worldX(longitude: Double): Double = longitude / FULL_TURN_DEGREES + HALF

/** Web Mercator, as a fraction of the world's height: 0 at the top of the map, 1 at the bottom. */
private fun worldY(latitude: Double): Double {
    val sine = sin(latitude.coerceIn(-MAX_MERCATOR_LATITUDE, MAX_MERCATOR_LATITUDE) * RADIANS_PER_DEGREE)
    return HALF - ln((1 + sine) / (1 - sine)) / TWO_TURNS_RADIANS
}
