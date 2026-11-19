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

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Badge
import androidx.compose.material3.BadgedBox
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.SideEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.key
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.google.android.gms.maps.model.LatLng
import com.google.maps.android.clustering.Cluster
import com.google.maps.android.clustering.ClusterManager
import com.google.maps.android.clustering.algo.NonHierarchicalDistanceBasedAlgorithm
import com.google.maps.android.clustering.algo.NonHierarchicalViewBasedAlgorithm
import com.google.maps.android.clustering.algo.PreCachingAlgorithmDecorator
import com.google.maps.android.clustering.algo.ScreenBasedAlgorithmAdapter
import com.google.maps.android.clustering.view.DefaultClusterRenderer
import com.google.maps.android.compose.Circle
import com.google.maps.android.compose.MapsComposeExperimentalApi
import com.google.maps.android.compose.clustering.Clustering
import com.google.maps.android.compose.clustering.ClusteringMarkerProperties
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.yield
import org.jetbrains.compose.resources.stringResource
import org.meshtastic.app.map.NodeMarkerSet
import org.meshtastic.app.map.model.LiveMapNodes
import org.meshtastic.app.map.model.NodeClusterItem
import org.meshtastic.app.map.relativeTimeBuckets
import org.meshtastic.core.common.util.nowSeconds
import org.meshtastic.core.model.Node
import org.meshtastic.core.model.nodeColorsFromNum
import org.meshtastic.core.model.util.TimeConstants
import org.meshtastic.core.resources.Res
import org.meshtastic.core.resources.now
import org.meshtastic.core.resources.unknown
import org.meshtastic.feature.map.stackBadgeLabel

const val MIN_CLUSTER_SIZE = 10

/** The clustering algorithms' own default `maxDistanceBetweenClusteredItems`, in dp; about one chip's width. */
const val CLUSTER_MAX_DISTANCE_DP = 100

// Matches the MapLibre map's precision fill, so both maps shade a degraded position alike.
private const val PRECISION_FILL_ALPHA = 0.15f

// BadgedBox draws the badge past its anchor's top end corner, and a marker is rasterized at its measured size, so the
// marker is padded by this much above and to the sides to keep the badge unclipped. Never below: the marker anchors on
// its bottom edge, and bottom padding would lift every chip off its point.
private val STACK_BADGE_INSET = 12.dp

// A marker's title is refreshed this many at a time, with a frame let through in between.
private const val TITLE_REFRESH_BATCH = 50

/** A node's chip, with a Material 3 count badge when it stands for [stackSize] nodes at one spot. */
@Composable
private fun StackedNodeChip(node: Node, stackSize: Int, shouldPulse: () -> Boolean, modifier: Modifier = Modifier) {
    Box(modifier.padding(start = STACK_BADGE_INSET, top = STACK_BADGE_INSET, end = STACK_BADGE_INSET)) {
        BadgedBox(
            badge = {
                if (stackSize > 1) {
                    // Secondary rather than the default error color: a count of nodes is not an alert.
                    Badge(containerColor = MaterialTheme.colorScheme.secondary) { Text(stackBadgeLabel(stackSize)) }
                }
            },
        ) {
            PulsingNodeChip(node = node, shouldPulse = shouldPulse)
        }
    }
}

/**
 * The chip a marker is drawn from. It reads its node from [liveNodes], so it redraws when that node changes and is left
 * alone when any other does.
 */
@OptIn(MapsComposeExperimentalApi::class)
@Composable
private fun NodeMarkerChip(item: NodeClusterItem, liveNodes: LiveMapNodes, isOnScreen: (LatLng) -> Boolean) {
    ClusteringMarkerProperties(zIndex = item.zIndex)
    val node by remember(liveNodes, item.nodeNum) { liveNodes.state(item.nodeNum) }
    node?.let {
        StackedNodeChip(
            node = it,
            stackSize = item.stackMemberNums.size,
            // A pulse redraws the marker's bitmap every frame for a second; off screen there is nobody to see it.
            shouldPulse = { isOnScreen(item.position) },
        )
    }
}

/** The circle that says where a node with reduced position precision could be. */
@Composable
private fun PrecisionCircle(item: NodeClusterItem) {
    val precisionMeters = item.precisionMeters
    if (precisionMeters != null && precisionMeters > 0) {
        val color = Color(nodeColorsFromNum(item.nodeNum).second)
        Circle(
            center = item.position,
            radius = precisionMeters,
            fillColor = color.copy(alpha = PRECISION_FILL_ALPHA),
            strokeColor = color,
            strokeWidth = 2f,
            zIndex = 0f,
        )
    }
}

/**
 * Threshold for switching to [NonHierarchicalViewBasedAlgorithm], matching that class's own KDoc describing itself as
 * being for "large numbers of items (>1000 markers)".
 */
private const val VIEW_BASED_ALGORITHM_NODE_THRESHOLD = 1000

/**
 * Renders the nodes as a clustered marker layer inside a `GoogleMap` content scope.
 *
 * [nodeMarkers] is collected here, not by the caller, so a packet recomposes this function and the one chip whose node
 * it changed. The clustering itself only sees [NodeMarkerSet.items], which stays the same list until a marker has to
 * move, appear or go.
 *
 * Must be called from within a `GoogleMap { }` content lambda.
 */
@Suppress("LongParameterList")
@Composable
fun NodeClusterMarkers(
    nodeMarkers: StateFlow<NodeMarkerSet>,
    liveNodes: LiveMapNodes,
    showPrecisionCircle: Boolean,
    isOnScreen: (LatLng) -> Boolean,
    navigateToNodeDetails: (Int) -> Unit,
    onClusterClick: (Cluster<NodeClusterItem>) -> Boolean,
    onStackClick: (List<Int>) -> Unit,
) {
    val markerSet by nodeMarkers.collectAsStateWithLifecycle()
    val unknownText = stringResource(Res.string.unknown)
    val nowText = stringResource(Res.string.now)
    // Before the clustering can ask for a chip or a title: it works from the items below on a later frame.
    SideEffect {
        liveNodes.setRelativeTimeTexts(unknown = unknownText, now = nowText)
        liveNodes.update(markerSet.nodes)
    }
    NodeClusterLayer(
        items = markerSet.items,
        liveNodes = liveNodes,
        showPrecisionCircle = showPrecisionCircle,
        isOnScreen = isOnScreen,
        navigateToNodeDetails = navigateToNodeDetails,
        onClusterClick = onClusterClick,
        onStackClick = onStackClick,
    )
}

/** Skipped while [items] is the list it was last given, which is what keeps a packet away from the clustering. */
@Suppress("LongParameterList")
@OptIn(MapsComposeExperimentalApi::class)
@Composable
private fun NodeClusterLayer(
    items: List<NodeClusterItem>,
    liveNodes: LiveMapNodes,
    showPrecisionCircle: Boolean,
    isOnScreen: (LatLng) -> Boolean,
    navigateToNodeDetails: (Int) -> Unit,
    onClusterClick: (Cluster<NodeClusterItem>) -> Boolean,
    onStackClick: (List<Int>) -> Unit,
) {
    val configuration = LocalConfiguration.current
    // One circle per stack, drawn whether or not the stack sits inside a cluster bubble, as the Apple and MapLibre maps
    // do: the circle says where the nodes could be, which a count bubble does not.
    if (showPrecisionCircle) {
        items.forEach { item -> key(item.nodeNum) { PrecisionCircle(item) } }
    }
    var clusterManager by remember { mutableStateOf<ClusterManager<NodeClusterItem>?>(null) }
    Clustering(
        items = items,
        onClusterClick = onClusterClick,
        // A tap goes where the MapLibre map's does: a stack lists its nodes, a single node opens its details.
        onClusterItemClick = { item ->
            if (item.stackMemberNums.size > 1) {
                onStackClick(item.stackMemberNums)
            } else {
                navigateToNodeDetails(item.nodeNum)
            }
            true
        },
        clusterItemContent = { item -> NodeMarkerChip(item, liveNodes, isOnScreen) },
        onClusterManager = { manager ->
            clusterManager = manager
            // The library renderer extends DefaultClusterRenderer; configure it once.
            val renderer = manager.renderer as? DefaultClusterRenderer<*>
            if (renderer != null && (renderer.minClusterSize != MIN_CLUSTER_SIZE || !renderer.showExactCount)) {
                renderer.minClusterSize = MIN_CLUSTER_SIZE
                // Exact counts, "1.2k" past a thousand: what the MapLibre map's point_count_abbreviated label and the
                // Apple map's cluster badge show, where the library's default buckets read "10+", "20+", "50+".
                renderer.showExactCount = true
                renderer.useCompactNumberFormatting = true
                manager.cluster()
            }
            // The clustering decides the bubbles and ChipMergingAlgorithm merges the chips it leaves out. Only the
            // clustering under it is swapped, and only on a threshold crossing: replacing re-migrates every item.
            val merging = manager.algorithm as? ChipMergingAlgorithm
            val clustering = merging?.delegate
            val needsViewBasedAlgorithm = items.size > VIEW_BASED_ALGORITHM_NODE_THRESHOLD
            when {
                needsViewBasedAlgorithm && clustering is NonHierarchicalViewBasedAlgorithm<*> ->
                    clustering.updateViewSize(configuration.screenWidthDp, configuration.screenHeightDp)

                needsViewBasedAlgorithm -> {
                    val viewBased =
                        NonHierarchicalViewBasedAlgorithm<NodeClusterItem>(
                            configuration.screenWidthDp,
                            configuration.screenHeightDp,
                        )
                    manager.setAlgorithm(ChipMergingAlgorithm(viewBased))
                }

                merging == null || clustering is NonHierarchicalViewBasedAlgorithm<*> -> {
                    // The library's own default: distance based, with the zoom levels either side kept ready.
                    val distanceBased =
                        PreCachingAlgorithmDecorator(NonHierarchicalDistanceBasedAlgorithm<NodeClusterItem>())
                    manager.setAlgorithm(ChipMergingAlgorithm(ScreenBasedAlgorithmAdapter(distanceBased)))
                }
            }
        },
    )
    clusterManager?.let { MarkerTitleRefresh(clusterManager = it) }
}

/**
 * Keeps the "heard ago" in each marker's title, which is what a screen reader reads out, moving with the clock.
 *
 * The clustering sets a title when it adds a marker and again whenever it lays the markers out. Between those, nothing
 * else would touch it: a node's item no longer changes just because a minute went by.
 */
@Composable
private fun MarkerTitleRefresh(clusterManager: ClusterManager<NodeClusterItem>) {
    val minute = rememberRelativeTimeBucket()
    LaunchedEffect(clusterManager, minute) {
        @Suppress("UNCHECKED_CAST")
        val renderer = clusterManager.renderer as? DefaultClusterRenderer<NodeClusterItem> ?: return@LaunchedEffect
        // The chips now on the map, which are not the items the map was given: nearby ones are merged into one.
        clusterManager.markerCollection.getMarkers().toList().forEachIndexed { index, marker ->
            // Null for a marker removed since the list was copied.
            renderer.getClusterItem(marker)?.let { item ->
                val title = item.title
                if (marker.title != title) marker.title = title
            }
            if (index % TITLE_REFRESH_BATCH == TITLE_REFRESH_BATCH - 1) yield()
        }
    }
}

@Composable
private fun rememberRelativeTimeBucket(): Long {
    val buckets = remember { relativeTimeBuckets() }
    return buckets.collectAsStateWithLifecycle(initialValue = nowSeconds / TimeConstants.SECONDS_PER_MINUTE).value
}
