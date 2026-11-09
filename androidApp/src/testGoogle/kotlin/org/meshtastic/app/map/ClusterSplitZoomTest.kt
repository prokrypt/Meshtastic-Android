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

import com.google.android.gms.maps.model.LatLng
import org.junit.Test
import kotlin.math.cos
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ClusterSplitZoomTest {

    private fun eastOf(origin: LatLng, meters: Double) =
        LatLng(origin.latitude, origin.longitude + meters / (111_320.0 * cos(Math.toRadians(origin.latitude))))

    private val oakland = LatLng(37.8, -122.2)

    @Test
    fun `nodes at one spot never split`() {
        assertNull(clusterSplitZoom(listOf(oakland, oakland)))
    }

    @Test
    fun `nodes a few meters apart are listed rather than zoomed past the deepest zoom`() {
        assertNull(clusterSplitZoom(listOf(oakland, eastOf(oakland, 4.0))))
    }

    @Test
    fun `nearby nodes split at street zoom`() {
        assertEquals(21, clusterSplitZoom(listOf(oakland, eastOf(oakland, 10.0))))
        assertEquals(14, clusterSplitZoom(listOf(oakland, eastOf(oakland, 1_000.0))))
    }
}
