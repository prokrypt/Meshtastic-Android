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
package org.meshtastic.app.map.model

import androidx.compose.runtime.Immutable
import com.google.android.gms.maps.model.LatLng
import com.google.maps.android.clustering.ClusterItem

/**
 * One marker the Google map's clustering places: a node, or the node drawn for several at one spot.
 *
 * Two items are equal while everything the clustering and the marker are built from is equal: which node, where it is,
 * how high it draws, its precision circle, and which nodes share its spot. The rest of the node (when it was last
 * heard, its name, its readings) is not held here. The chip and the marker's title read it from [liveNodes] when they
 * are drawn, so a packet that moves no node leaves the list of items equal to the one before it, and the map keeps its
 * markers instead of clustering and redrawing every one of them.
 */
@Immutable
class NodeClusterItem(
    val nodeNum: Int,
    val nodePosition: LatLng,
    /** [org.meshtastic.feature.map.MapNodePolicy.priorityOf] this node. */
    val priority: Int,
    /** Radius of the node's precision circle, or null when its position is reported in full. */
    val precisionMeters: Double?,
    /** Every node this marker stands for, this one included, when more than one is; empty otherwise. */
    val stackMemberNums: List<Int> = emptyList(),
    private val liveNodes: LiveMapNodes,
) : ClusterItem {
    override val position: LatLng
        get() = nodePosition

    /** Read when asked for, so the "heard ago" in it is as of then. */
    override val title: String?
        get() = liveNodes.title(nodeNum)

    override val snippet: String?
        get() = liveNodes.snippet(nodeNum)

    override val zIndex: Float
        get() = priority.toFloat()

    /**
     * This item standing for every node in [members] as well as its own: one chip where theirs would overlap.
     *
     * @param members the items merged into one, this one among them.
     */
    fun mergedWith(members: List<NodeClusterItem>): NodeClusterItem = NodeClusterItem(
        nodeNum = nodeNum,
        nodePosition = nodePosition,
        priority = priority,
        precisionMeters = precisionMeters,
        stackMemberNums = members.flatMap { it.stackMemberNums.ifEmpty { listOf(it.nodeNum) } },
        liveNodes = liveNodes,
    )

    override fun equals(other: Any?): Boolean = this === other ||
        (
            other is NodeClusterItem &&
                nodeNum == other.nodeNum &&
                priority == other.priority &&
                nodePosition == other.nodePosition &&
                precisionMeters == other.precisionMeters &&
                stackMemberNums == other.stackMemberNums
            )

    // The clustering hashes every item several times a pass; a node is in one item, so its number spreads them.
    override fun hashCode(): Int = nodeNum

    override fun toString(): String =
        "NodeClusterItem(nodeNum=$nodeNum, position=$nodePosition, priority=$priority, stack=$stackMemberNums)"
}
