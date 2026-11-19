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

import android.app.Application
import androidx.compose.runtime.snapshots.Snapshot
import androidx.lifecycle.SavedStateHandle
import androidx.test.core.app.ApplicationProvider
import com.google.android.gms.maps.model.CameraPosition
import com.google.android.gms.maps.model.LatLng
import com.google.maps.android.compose.MapType
import dev.mokkery.MockMode
import dev.mokkery.answering.returns
import dev.mokkery.every
import dev.mokkery.mock
import io.ktor.client.HttpClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import kotlinx.serialization.json.Json
import okio.Path.Companion.toOkioPath
import org.junit.runner.RunWith
import org.meshtastic.app.map.prefs.map.GoogleCameraPosition
import org.meshtastic.app.map.prefs.map.GoogleMapSelectionPrefs
import org.meshtastic.app.map.prefs.map.GoogleMapsPrefs
import org.meshtastic.app.map.tiles.MapTileHttpClient
import org.meshtastic.core.di.CoroutineDispatchers
import org.meshtastic.core.network.repository.NetworkRepository
import org.meshtastic.core.repository.PacketRepository
import org.meshtastic.core.testing.FakeBuildConfigProvider
import org.meshtastic.core.testing.FakeLocaleUnitsProvider
import org.meshtastic.core.testing.FakeMapPrefs
import org.meshtastic.core.testing.FakeMapTileProviderPrefs
import org.meshtastic.core.testing.FakeNodeRepository
import org.meshtastic.core.testing.FakeNotificationPrefs
import org.meshtastic.core.testing.FakeRadioConfigRepository
import org.meshtastic.core.testing.FakeRadioController
import org.meshtastic.core.testing.FakeUiPrefs
import org.meshtastic.feature.map.layers.LayerOpacityStore
import org.meshtastic.feature.map.layers.MapLayersManager
import org.meshtastic.feature.map.tiles.CustomTileProviderRepositoryImpl
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * [MapViewModel.nodeMarkers] end to end: what the Google map's marker layer is handed as packets arrive and the camera
 * moves.
 *
 * The ticker that rebuilds the markers each minute never finishes, so these step the scheduler with `runCurrent` and
 * never wait for it to go idle.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35], application = Application::class)
class MapNodeMarkersFlowTest {

    @Test
    fun `a packet that moves no node keeps the map's items and still publishes the node it came from`() =
        withMap { nodeRepository, viewModel ->
            val nodes = testNodes(NODE_COUNT)
            nodeRepository.setNodes(nodes)
            runCurrent()
            val before = viewModel.nodeMarkers.value
            assertTrue(before.items.isNotEmpty())
            assertEquals(NODE_COUNT, before.nodes.size)

            // A telemetry packet: the node was heard again, at the place it was already drawn.
            val heard = nodes[2].copy(lastHeard = 1_700_000_500, snr = 4.5f)
            nodeRepository.setNodes(nodes.map { if (it.num == heard.num) heard else it })
            runCurrent()
            val after = viewModel.nodeMarkers.value

            assertNotSame(before, after)
            assertSame(before.items, after.items, "the clustering is handed a new list only when a marker changed")
            assertSame(heard, after.nodes[heard.num], "the chip and the title read the node as it now is")
        }

    @Test
    fun `a node that moves changes the map's items`() = withMap { nodeRepository, viewModel ->
        val nodes = testNodes(NODE_COUNT)
        nodeRepository.setNodes(nodes)
        runCurrent()
        val before = viewModel.nodeMarkers.value

        // Half a degree: a move no merge distance takes in.
        val moved =
            nodes[0].copy(
                position =
                nodes[0].position.newBuilder().also { it.latitude_i = (MOVED_LATITUDE * 1e7).toInt() }.build(),
            )
        nodeRepository.setNodes(nodes.map { if (it.num == moved.num) moved else it })
        runCurrent()
        val after = viewModel.nodeMarkers.value

        assertNotSame(before.items, after.items)
        val movedItem = after.items.single { it.nodeNum == moved.num }
        assertEquals(MOVED_LATITUDE, movedItem.position.latitude, absoluteTolerance = 1e-6)
    }

    @Test
    fun `moving or zooming the camera does not rebuild the markers`() = withMap { nodeRepository, viewModel ->
        // Which chips merge at a zoom is the clustering's to decide, so the items it is given do not follow the camera.
        nodeRepository.setNodes(testNodes(NODE_COUNT))
        runCurrent()
        val before = viewModel.nodeMarkers.value
        assertEquals(NODE_COUNT, before.items.size)

        viewModel.cameraPositionState.position = CameraPosition.fromLatLngZoom(LatLng(21.0, -157.0), CLOSE_ZOOM)
        Snapshot.sendApplyNotifications()
        runCurrent()

        assertSame(before, viewModel.nodeMarkers.value)
    }

    /** Runs [block] against a view model whose markers are being collected, as the map screen collects them. */
    private fun withMap(block: suspend TestScope.(FakeNodeRepository, MapViewModel) -> Unit) = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val dispatchers = CoroutineDispatchers(dispatcher, dispatcher, dispatcher)
        Dispatchers.setMain(dispatcher)
        val httpClient = HttpClient()
        val layersDir = createTempDirectory("map-layers")
        try {
            val application = ApplicationProvider.getApplicationContext<Application>()
            val mapPrefs = FakeMapPrefs()
            val mapTileProviderPrefs = FakeMapTileProviderPrefs()
            val nodeRepository = FakeNodeRepository()
            val packetRepository = mock<PacketRepository>(MockMode.autofill)
            every { packetRepository.getWaypoints() } returns flowOf(emptyList())
            val networkRepository = mock<NetworkRepository>(MockMode.autofill)
            every { networkRepository.networkAvailable } returns flowOf(true)
            val viewModel =
                MapViewModel(
                    application = application,
                    dispatchers = dispatchers,
                    mapLayersManager =
                    MapLayersManager(
                        dispatchers = dispatchers,
                        httpClient = httpClient,
                        mapPrefs = mapPrefs,
                        // The real location reads a global application context this test never installs.
                        layersDir = layersDir.toOkioPath(),
                    ),
                    layerOpacityStore = LayerOpacityStore(mapPrefs, dispatchers),
                    mapPrefs = mapPrefs,
                    googleMapsPrefs = NoSavedCameraPrefs(),
                    nodeRepository = nodeRepository,
                    packetRepository = packetRepository,
                    radioConfigRepository = FakeRadioConfigRepository(),
                    radioController = FakeRadioController(),
                    customTileProviderRepository =
                    CustomTileProviderRepositoryImpl(
                        json = Json { ignoreUnknownKeys = true },
                        dispatchers = dispatchers,
                        mapTileProviderPrefs = mapTileProviderPrefs,
                    ),
                    mapTileProviderPrefs = mapTileProviderPrefs,
                    mapTileHttpClient = MapTileHttpClient(application, FakeBuildConfigProvider()),
                    uiPrefs = FakeUiPrefs(),
                    notificationPrefs = FakeNotificationPrefs(),
                    savedStateHandle = SavedStateHandle(),
                    localeUnitsProvider = FakeLocaleUnitsProvider(),
                    networkRepository = networkRepository,
                )
            backgroundScope.launch { viewModel.nodeMarkers.collect {} }
            runCurrent()
            block(nodeRepository, viewModel)
        } finally {
            try {
                httpClient.close()
                layersDir.toFile().deleteRecursively()
            } finally {
                Dispatchers.resetMain()
            }
        }
    }

    /** No saved camera, so the map opens at the view model's own default zoom. */
    private class NoSavedCameraPrefs : GoogleMapsPrefs {
        override val selectedGoogleMapType = MutableStateFlow<String?>(MapType.NORMAL.name)
        override val selectedCustomTileUrl = MutableStateFlow<String?>(null)
        override val cameraPosition = MutableStateFlow<GoogleCameraPosition?>(null)

        override fun setSelectedGoogleMapType(value: String?) {
            selectedGoogleMapType.value = value
        }

        override fun setSelectedCustomTileUrl(value: String?) {
            selectedCustomTileUrl.value = value
        }

        override suspend fun awaitMapSelection() = GoogleMapSelectionPrefs(
            mapType = selectedGoogleMapType.value ?: MapType.NORMAL.name,
            customTileUrl = selectedCustomTileUrl.value,
        )

        override fun setCameraPosition(value: GoogleCameraPosition) {
            cameraPosition.value = value
        }
    }

    private companion object {
        const val NODE_COUNT = 5
        const val CLOSE_ZOOM = 21f
        const val MOVED_LATITUDE = 21.5
    }
}
