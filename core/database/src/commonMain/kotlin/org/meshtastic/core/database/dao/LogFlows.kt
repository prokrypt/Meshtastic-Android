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

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.mapLatest
import org.meshtastic.core.database.entity.MeshLog

/**
 * Emits what [MeshLogDao.getLogsFrom] emits, but reads the rows only when [fromNum]'s own logs changed.
 *
 * Room re-runs a query on every write to its table, and while a screen is open every received packet adds a log row. A
 * node's detail screen holds several of these queries of up to [maxItem] rows each, so a packet from any node on the
 * mesh re-read and decoded this node's whole history. This flow watches [MeshLogDao.logStampFrom] instead, which costs
 * an index lookup, and runs the full query only when the stamp moves. A write landing between the stamp and the read
 * moves the stamp again, so the flow converges on the table like the query it replaces.
 */
@OptIn(ExperimentalCoroutinesApi::class)
fun MeshLogDao.observeLogsFrom(fromNum: Int, portNum: Int, maxItem: Int): Flow<List<MeshLog>> =
    logStampFrom(fromNum).distinctUntilChanged().mapLatest { getLogsSnapshot(fromNum, portNum, maxItem) }

/** [observeLogsFrom] for [MeshLogDao.getLogsOnPort]: reads the rows only when a log on [portNum] changed. */
@OptIn(ExperimentalCoroutinesApi::class)
fun MeshLogDao.observeLogsOnPort(portNum: Int, excludeFromNum: Int, maxItem: Int): Flow<List<MeshLog>> =
    logStampOnPort(portNum).distinctUntilChanged().mapLatest {
        getLogsOnPortSnapshot(portNum, excludeFromNum, maxItem)
    }
