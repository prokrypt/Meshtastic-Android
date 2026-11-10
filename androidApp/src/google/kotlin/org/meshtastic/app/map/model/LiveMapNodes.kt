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

import androidx.compose.runtime.MutableState
import androidx.compose.runtime.Stable
import androidx.compose.runtime.State
import androidx.compose.runtime.mutableStateOf
import org.meshtastic.core.model.Node
import org.meshtastic.core.model.effectiveLongName
import org.meshtastic.core.ui.util.formatAgo
import java.util.concurrent.ConcurrentHashMap

/**
 * The node behind each marker on the Google map, as last reported.
 *
 * A marker's chip, and the title a screen reader gives the marker, read their node from here rather than from the
 * [NodeClusterItem], which only says where the marker goes. Each node has a snapshot state of its own, so a packet
 * redraws the one chip whose node it changed.
 *
 * Written on the main thread, where the markers live: [update] runs after a composition and [state] in a marker's own.
 * Reading is safe from any thread, since the clustering works its items on its own threads and nothing here says it
 * will never ask one for its [title] there.
 */
@Stable
class LiveMapNodes {
    // Never pruned. A marker can outlive its node by a clustering pass and then needs the state it already reads to
    // stay the one that is updated; the map holds at most one small entry per node the mesh has shown.
    private val states = ConcurrentHashMap<Int, MutableState<Node?>>()
    private var applied: Map<Int, Node>? = null

    @Volatile private var unknownText = ""

    @Volatile private var nowText = ""

    /** The words [title] uses for a position with no time and for one under a minute old. */
    fun setRelativeTimeTexts(unknown: String, now: String) {
        unknownText = unknown
        nowText = now
    }

    /** Publishes [nodes], by number. Only a node that differs from the one already held invalidates its readers. */
    fun update(nodes: Map<Int, Node>) {
        if (nodes === applied) return
        applied = nodes
        for ((num, node) in nodes) {
            val state = states[num]
            if (state == null) {
                states[num] = mutableStateOf(node)
            } else if (state.value !== node) {
                state.value = node
            }
        }
    }

    /** The node numbered [nodeNum], as snapshot state; null until [update] has published one. */
    fun state(nodeNum: Int): State<Node?> = states.getOrPut(nodeNum) { mutableStateOf(null) }

    fun node(nodeNum: Int): Node? = states[nodeNum]?.value

    /** The marker's title: the node's short name and how long ago its position was taken. */
    fun title(nodeNum: Int): String? =
        node(nodeNum)?.let { "${it.user.short_name} ${formatAgo(it.position.time, unknownText, nowText)}" }

    fun snippet(nodeNum: Int): String? = node(nodeNum)?.effectiveLongName
}
