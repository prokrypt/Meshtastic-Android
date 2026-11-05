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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.meshtastic.core.database.entity.NodeWithRelations

private class CachedNodeRow(val fingerprint: String, val row: NodeWithRelations)

/**
 * The node database keyed by number, emitting what [NodeInfoDao.nodeDBbyNum] emits: it watches the same tables, and
 * each map has the same keys, values and order.
 *
 * [NodeInfoDao.nodeDBbyNum] decodes every row's protobuf blobs on each change, and every received packet changes its
 * sender's row, so a busy mesh paid for a full-table decode per packet. This flow instead watches the cheap
 * [NodeInfoDao.nodeFingerprints] query and fetches full rows only for nodes whose fingerprint changed (usually the one
 * sender). Rows that did not change keep their previous [NodeWithRelations] instance, so downstream mappers can skip
 * them by identity.
 *
 * The first emission, and any emission where more rows changed than one `IN` query can bind, reads the whole table with
 * [NodeInfoDao.nodeDBbyNumSnapshot] as before. A write landing between the fingerprint read and the row fetch triggers
 * another emission, which fetches the row again because its cached fingerprint is stale, so the map converges on the
 * database exactly like the Room flow it replaces.
 */
fun NodeInfoDao.observeNodeDBbyNum(): Flow<Map<Int, NodeWithRelations>> = flow {
    var cache: Map<Int, CachedNodeRow> = emptyMap()
    nodeFingerprints().collect { fingerprints ->
        val changed = HashSet<Int>()
        for (row in fingerprints) {
            if (cache[row.num]?.fingerprint != row.fingerprint) changed += row.num
        }
        val fetched: Map<Int, NodeWithRelations> =
            when {
                changed.isEmpty() -> emptyMap()
                cache.isEmpty() || changed.size > NodeInfoDao.MAX_BIND_PARAMS -> nodeDBbyNumSnapshot()
                else -> getNodesWithRelationsByNums(changed.toList()).associateBy { it.node.num }
            }
        val nextCache = LinkedHashMap<Int, CachedNodeRow>(fingerprints.size)
        val result = LinkedHashMap<Int, NodeWithRelations>(fingerprints.size)
        for (row in fingerprints) {
            // A changed row missing from the fetch was deleted after the fingerprint read; the next emission drops it.
            val entry =
                if (row.num in changed) fetched[row.num]?.let { CachedNodeRow(row.fingerprint, it) } else cache[row.num]
            if (entry != null) {
                nextCache[row.num] = entry
                result[row.num] = entry.row
            }
        }
        cache = nextCache
        emit(result)
    }
}
