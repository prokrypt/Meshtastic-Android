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

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.meshtastic.app.map.model.LiveMapNodes
import org.meshtastic.app.map.model.NodeClusterItem
import org.meshtastic.core.common.util.nowSeconds
import org.meshtastic.core.model.Node
import org.meshtastic.core.model.util.TimeConstants
import org.meshtastic.core.model.util.precisionRadiusMetersOrNull
import org.meshtastic.feature.map.BaseMapViewModel.MapFilterState
import org.meshtastic.feature.map.MapNodePolicy
import org.meshtastic.feature.map.stackCoincident

/** What the Google map's node layer draws. [MapViewModel.nodeMarkers] builds one off the main thread per change. */
data class NodeMarkerSet(
    /**
     * One per spot with nodes on it. The very list the set before it held, for as long as no marker moved, appeared,
     * left or changed what it stands for, so the map can tell at a glance that its markers are still right.
     */
    val items: List<NodeClusterItem>,
    /** Every node on the map by number: the ones [items] are drawn for and the ones stacked under them. */
    val nodes: Map<Int, Node>,
) {
    companion object {
        val Empty = NodeMarkerSet(items = emptyList(), nodes = emptyMap())
    }
}

/**
 * The markers for [nodes] under the map's filters.
 *
 * [previous] is the set built before this one. Its item list is handed on unchanged when the new one comes out equal,
 * which is what happens for a packet that only changes when a node was heard or what it reported.
 */
@Suppress("LongParameterList")
internal fun buildNodeMarkerSet(
    nodes: List<Node>,
    filterState: MapFilterState,
    nowSeconds: Long,
    ourNodeNum: Int?,
    myNodeNum: Int?,
    liveNodes: LiveMapNodes,
    previous: NodeMarkerSet? = null,
): NodeMarkerSet {
    // By node number rather than in the last-heard order the repository hands out. Stacking and the clustering both
    // anchor a group on the first node they meet, so an order that changes with every packet regroups the map with
    // every packet.
    val visible = MapNodePolicy.visibleNodes(nodes, filterState, nowSeconds, ourNodeNum).sortedBy { it.num }
    val items = buildNodeClusterItems(visible, myNodeNum, liveNodes)
    return NodeMarkerSet(
        items = previous?.items?.takeIf { it == items } ?: items,
        nodes = visible.associateBy { it.num },
    )
}

/**
 * One item per spot: nodes within a few meters of each other share one, since no zoom level separates them.
 *
 * Chips that merely sit close at the current zoom are not merged here. The map's clustering does that, once it has
 * decided its bubbles: see [org.meshtastic.app.map.component.ChipMergingAlgorithm].
 */
internal fun buildNodeClusterItems(nodes: List<Node>, myNodeNum: Int?, liveNodes: LiveMapNodes): List<NodeClusterItem> =
    stackCoincident(nodes, myNodeNum).map { stack ->
        val node = stack.top
        NodeClusterItem(
            nodeNum = node.num,
            nodePosition = node.position.toLatLng(),
            priority = MapNodePolicy.priorityOf(node, myNodeNum),
            precisionMeters = precisionRadiusMetersOrNull(node.position.precision_bits),
            stackMemberNums = if (stack.count > 1) stack.members.map { it.num } else emptyList(),
            liveNodes = liveNodes,
        )
    }

/** The current minute, again at each minute boundary: when the map's "heard ago" texts and time filters move on. */
internal fun relativeTimeBuckets(now: () -> Long = { nowSeconds }): Flow<Long> = flow {
    while (true) {
        val currentSeconds = now()
        emit(currentSeconds / TimeConstants.SECONDS_PER_MINUTE)
        val secondsUntilNextMinute =
            TimeConstants.SECONDS_PER_MINUTE - currentSeconds.mod(TimeConstants.SECONDS_PER_MINUTE)
        delay(secondsUntilNextMinute * TimeConstants.MS_PER_SEC)
    }
}
