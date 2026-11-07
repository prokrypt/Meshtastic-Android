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

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import org.meshtastic.core.database.MeshtasticDatabase
import org.meshtastic.core.database.entity.MyNodeEntity
import org.meshtastic.core.database.entity.NodeEntity
import org.meshtastic.core.database.getInMemoryDatabaseBuilder
import org.meshtastic.core.model.NodeSortOption
import org.meshtastic.proto.HardwareModel
import org.meshtastic.proto.User
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** [NodeInfoDao.getNodeNums] must pick and order exactly the nodes [NodeInfoDao.getNodes] returns. */
class NodeNumsQueryTest {
    private lateinit var database: MeshtasticDatabase
    private lateinit var dao: NodeInfoDao

    @BeforeTest
    fun setUp() {
        database = getInMemoryDatabaseBuilder().build()
        dao = database.nodeInfoDao()
    }

    @AfterTest
    fun tearDown() {
        database.close()
    }

    @Test
    fun everySortAndFilterMatchesTheFullQuery() = runTest {
        dao.setMyNodeInfo(myNode(LOCAL))
        dao.putAll((1..MESH_SIZE).map(::meshNode) + meshNode(LOCAL))

        var compared = 0
        for (sort in NodeSortOption.entries.map { it.sqlValue }) {
            for (includeUnknown in listOf(true, false)) {
                for (onlyDirect in listOf(true, false)) {
                    for (lastHeardMin in listOf(-1, BASE_TIME - MESH_SIZE / 2)) {
                        val full = dao.getNodes(sort, includeUnknown, onlyDirect, lastHeardMin).first()
                        val nums = dao.getNodeNums(sort, includeUnknown, onlyDirect, lastHeardMin).first()
                        assertEquals(full.map { it.node.num }, nums, "sort=$sort unknown=$includeUnknown")
                        compared += nums.size
                    }
                }
            }
        }
        assertTrue(compared > 0)
    }

    private fun myNode(num: Int) = MyNodeEntity(
        myNodeNum = num,
        model = "T1000E",
        firmwareVersion = "2.8.0",
        couldUpdate = false,
        shouldUpdate = false,
        currentPacketId = 1L,
        messageTimeoutMsec = 300000,
        minAppVersion = 1,
        maxChannels = 8,
        hasWifi = false,
    )

    /** Nodes that differ in every column the filters and sorts read. */
    private fun meshNode(num: Int): NodeEntity {
        val known = num % 7 != 0
        return NodeEntity(
            num = num,
            user =
            User.Builder()
                .also {
                    it.id = "!" + num.toString(16).padStart(8, '0')
                    it.long_name = "Node ${(num * 37) % 101}"
                    it.short_name = "N$num"
                    it.hw_model = if (known) HardwareModel.RAK4631 else HardwareModel.UNSET
                }
                .build(),
            longName = if (known) "Node ${(num * 37) % 101}" else null,
            shortName = if (known) "N$num" else null,
            latitude = if (num % 5 == 0) 0.0 else 37.7 + (num % 13) * 0.01,
            longitude = if (num % 5 == 0) 0.0 else -122.4 - (num % 11) * 0.01,
            lastHeard = BASE_TIME - num,
            channel = num % 3,
            viaMqtt = num % 4 == 0,
            hopsAway = if (num % 6 == 0) -1 else num % 4,
            isFavorite = num % 9 == 0,
            displayName = if (num % 8 == 0) "alias $num" else "",
        )
    }

    private companion object {
        const val MESH_SIZE = 60
        const val LOCAL = 1000
        const val BASE_TIME = 1_793_900_000
    }
}
