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
package org.meshtastic.core.data.manager

import dev.mokkery.MockMode
import dev.mokkery.answering.calls
import dev.mokkery.answering.returns
import dev.mokkery.every
import dev.mokkery.everySuspend
import dev.mokkery.matcher.any
import dev.mokkery.mock
import dev.mokkery.verify
import dev.mokkery.verify.VerifyMode
import dev.mokkery.verifySuspend
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.meshtastic.core.common.di.asServiceScope
import org.meshtastic.core.model.MyNodeInfo
import org.meshtastic.core.repository.MeshConnectionManager
import org.meshtastic.core.repository.NodeManager
import org.meshtastic.core.repository.RadioConfigRepository
import org.meshtastic.core.repository.RadioInterfaceService
import org.meshtastic.core.repository.RadioSessionContext
import org.meshtastic.core.repository.ServiceRepository
import org.meshtastic.core.testing.FakeMeshPrefs
import org.meshtastic.proto.Channel
import org.meshtastic.proto.ChannelSet
import org.meshtastic.proto.ChannelSettings
import org.meshtastic.proto.Config
import org.meshtastic.proto.DeviceUIConfig
import org.meshtastic.proto.LoRaRegionPresetMap
import org.meshtastic.proto.LocalConfig
import org.meshtastic.proto.LocalModuleConfig
import org.meshtastic.proto.ModuleConfig
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class MeshConfigHandlerImplTest {

    private companion object {
        const val MY_NODE_NUM = 0x1234
    }

    private val radioConfigRepository = mock<RadioConfigRepository>(MockMode.autofill)
    private val serviceRepository = mock<ServiceRepository>(MockMode.autofill)
    private val nodeManager = mock<NodeManager>(MockMode.autofill)
    private val connectionManager = mock<MeshConnectionManager>(MockMode.autofill)
    private val radioInterfaceService = mock<RadioInterfaceService>(MockMode.autofill)

    private val localConfigFlow = MutableStateFlow(LocalConfig.Builder().build())
    private val channelSetFlow = MutableStateFlow(ChannelSet.Builder().build())
    private val meshPrefs = FakeMeshPrefs()
    private val moduleConfigFlow = MutableStateFlow(LocalModuleConfig.Builder().build())

    private val testDispatcher = UnconfinedTestDispatcher()
    private val session = RadioSessionContext(generation = 1L, address = "tcp:test")

    private lateinit var handler: MeshConfigHandlerImpl

    @BeforeTest
    fun setUp() {
        every { radioConfigRepository.localConfigFlow } returns localConfigFlow
        every { radioConfigRepository.channelSetFlow } returns channelSetFlow
        every { nodeManager.myNodeNum } returns MutableStateFlow(MY_NODE_NUM)
        meshPrefs.nodeSyncs[MY_NODE_NUM] = 1_800_000_000L
        every { radioConfigRepository.moduleConfigFlow } returns moduleConfigFlow
        every { radioInterfaceService.runIfSessionActive(session, any()) } calls
            {
                @Suppress("UNCHECKED_CAST")
                val block = it.args[1] as () -> Unit
                block()
                true
            }
        everySuspend { radioInterfaceService.runWhileSessionActive(session, any(), any()) } calls
            {
                @Suppress("UNCHECKED_CAST")
                val block = it.args[2] as (suspend () -> Unit)
                block()
                true
            }
    }

    private fun createHandler(scope: CoroutineScope): MeshConfigHandlerImpl = MeshConfigHandlerImpl(
        radioConfigRepository = radioConfigRepository,
        serviceStateWriter = serviceRepository,
        nodeManager = nodeManager,
        connectionManager = lazy { this.connectionManager },
        radioInterfaceService = radioInterfaceService,
        meshPrefs = meshPrefs,
        scope = scope.asServiceScope(),
    )

    // ---------- start and flow wiring ----------

    @Test
    fun `start wires localConfig flow from repository`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            LocalConfig.Builder()
                .also { wb ->
                    wb.device =
                        Config.DeviceConfig.Builder()
                            .also { wb -> wb.role = Config.DeviceConfig.Role.ROUTER }
                            .build()
                }
                .build()
        localConfigFlow.value = config
        advanceUntilIdle()

        assertEquals(config, handler.localConfig.value)
    }

    @Test
    fun `start wires moduleConfig flow from repository`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            LocalModuleConfig.Builder()
                .also { wb -> wb.mqtt = ModuleConfig.MQTTConfig.Builder().also { wb -> wb.enabled = true }.build() }
                .build()
        moduleConfigFlow.value = config
        advanceUntilIdle()

        assertEquals(config, handler.moduleConfig.value)
    }

    // ---------- handleDeviceConfig ----------

    @Test
    fun `handleDeviceConfig persists config and updates progress`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            Config.Builder()
                .also { wb ->
                    wb.device =
                        Config.DeviceConfig.Builder()
                            .also { wb -> wb.role = Config.DeviceConfig.Role.CLIENT }
                            .build()
                }
                .build()
        handler.handleDeviceConfig(config, session)
        advanceUntilIdle()

        verifySuspend { radioConfigRepository.setLocalConfig(config) }
        verify { serviceRepository.setConnectionProgress("Device config received") }
    }

    @Test
    fun `stale session cannot persist config or advance handshake`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            Config.Builder()
                .also { wb ->
                    wb.device =
                        Config.DeviceConfig.Builder()
                            .also { wb -> wb.role = Config.DeviceConfig.Role.CLIENT }
                            .build()
                }
                .build()
        every { radioInterfaceService.runIfSessionActive(session, any()) } returns false

        assertFalse(handler.handleDeviceConfig(config, session))
        advanceUntilIdle()

        verifySuspend(mode = VerifyMode.exactly(0)) { radioConfigRepository.setLocalConfig(any()) }
        verify(mode = VerifyMode.exactly(0)) { serviceRepository.setConnectionProgress(any()) }
        verify(mode = VerifyMode.exactly(0)) { connectionManager.onHandshakeProgress() }
    }

    @Test
    fun `active session persists device config and advances handshake`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            Config.Builder()
                .also { wb ->
                    wb.device =
                        Config.DeviceConfig.Builder()
                            .also { wb -> wb.role = Config.DeviceConfig.Role.CLIENT }
                            .build()
                }
                .build()
        every { radioInterfaceService.runIfSessionActive(session, any()) } calls
            {
                @Suppress("UNCHECKED_CAST")
                val block = it.args[1] as () -> Unit
                block()
                true
            }
        everySuspend { radioInterfaceService.runWhileSessionActive(session, any(), any()) } calls
            {
                @Suppress("UNCHECKED_CAST")
                val block = it.args[2] as (suspend () -> Unit)
                block()
                true
            }

        assertTrue(handler.handleDeviceConfig(config, session))
        advanceUntilIdle()

        verifySuspend { radioConfigRepository.setLocalConfig(config) }
        verify { serviceRepository.setConnectionProgress("Device config received") }
        verify { connectionManager.onHandshakeProgress() }
    }

    @Test
    fun `revoked session cannot complete queued config persistence`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            Config.Builder()
                .also { wb ->
                    wb.device =
                        Config.DeviceConfig.Builder()
                            .also { wb -> wb.role = Config.DeviceConfig.Role.CLIENT }
                            .build()
                }
                .build()
        every { radioInterfaceService.runIfSessionActive(session, any()) } calls
            {
                @Suppress("UNCHECKED_CAST")
                val block = it.args[1] as () -> Unit
                block()
                true
            }
        everySuspend { radioInterfaceService.runWhileSessionActive(session, any(), any()) } returns false

        handler.handleDeviceConfig(config, session)
        advanceUntilIdle()

        verifySuspend(mode = VerifyMode.exactly(0)) { radioConfigRepository.setLocalConfig(any()) }
        verify { serviceRepository.setConnectionProgress("Device config received") }
        verify { connectionManager.onHandshakeProgress() }
    }

    @Test
    fun `handleDeviceConfig handles all config variants`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val configs =
            listOf(
                Config.Builder().also { wb -> wb.position = Config.PositionConfig.Builder().build() }.build(),
                Config.Builder().also { wb -> wb.power = Config.PowerConfig.Builder().build() }.build(),
                Config.Builder().also { wb -> wb.network = Config.NetworkConfig.Builder().build() }.build(),
                Config.Builder().also { wb -> wb.display = Config.DisplayConfig.Builder().build() }.build(),
                Config.Builder().also { wb -> wb.lora = Config.LoRaConfig.Builder().build() }.build(),
                Config.Builder().also { wb -> wb.bluetooth = Config.BluetoothConfig.Builder().build() }.build(),
                Config.Builder().also { wb -> wb.security = Config.SecurityConfig.Builder().build() }.build(),
            )

        for (config in configs) {
            handler.handleDeviceConfig(config, session)
            advanceUntilIdle()
        }

        // All should have been persisted (7 configs)
        verifySuspend { radioConfigRepository.setLocalConfig(any()) }
    }

    // ---------- handleModuleConfig ----------

    @Test
    fun `handleModuleConfig persists config and updates progress`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            ModuleConfig.Builder()
                .also { wb -> wb.mqtt = ModuleConfig.MQTTConfig.Builder().also { wb -> wb.enabled = true }.build() }
                .build()
        handler.handleModuleConfig(config, session)
        advanceUntilIdle()

        verifySuspend { radioConfigRepository.setLocalModuleConfig(config) }
        verify { serviceRepository.setConnectionProgress("Module config received") }
    }

    @Test
    fun `handleModuleConfig with statusmessage updates node status`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val myNum = 123
        every { nodeManager.myNodeNum } returns MutableStateFlow<Int?>(myNum)

        val config =
            ModuleConfig.Builder()
                .also { wb ->
                    wb.statusmessage =
                        ModuleConfig.StatusMessageConfig.Builder().also { wb -> wb.node_status = "Active" }.build()
                }
                .build()
        handler.handleModuleConfig(config, session)
        advanceUntilIdle()

        verifySuspend { nodeManager.updateNodeStatusAndPersist(myNum, "Active") }
    }

    @Test
    fun `handleModuleConfig with statusmessage skipped when myNodeNum is null`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        every { nodeManager.myNodeNum } returns MutableStateFlow<Int?>(null)

        val config =
            ModuleConfig.Builder()
                .also { wb ->
                    wb.statusmessage =
                        ModuleConfig.StatusMessageConfig.Builder().also { wb -> wb.node_status = "Active" }.build()
                }
                .build()
        handler.handleModuleConfig(config, session)
        advanceUntilIdle()
        // No crash — updateNodeStatus should not be called
    }

    @Test
    fun `module config remains persisted when node status persistence fails`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val myNum = 123
        val config =
            ModuleConfig.Builder()
                .also { wb ->
                    wb.statusmessage =
                        ModuleConfig.StatusMessageConfig.Builder().also { wb -> wb.node_status = "Active" }.build()
                }
                .build()
        every { nodeManager.myNodeNum } returns MutableStateFlow<Int?>(myNum)
        everySuspend { nodeManager.updateNodeStatusAndPersist(myNum, "Active") } calls
            {
                throw IllegalStateException("forced node-status failure")
            }

        handler.handleModuleConfig(config, session)
        advanceUntilIdle()

        verifySuspend(mode = VerifyMode.exactly(1)) { radioConfigRepository.setLocalModuleConfig(config) }
        verifySuspend(mode = VerifyMode.exactly(1)) { nodeManager.updateNodeStatusAndPersist(myNum, "Active") }
    }

    @Test
    fun `module config cancellation cancels node status persistence job`() = runTest(testDispatcher) {
        val parentJob = Job()
        val handlerScope = CoroutineScope(parentJob + testDispatcher)
        handler = createHandler(handlerScope)
        val existingJobs = parentJob.children.toSet()
        val statusStarted = CompletableDeferred<Unit>()
        val releaseStatus = CompletableDeferred<Unit>()
        val myNum = 123
        val config =
            ModuleConfig.Builder()
                .also { wb ->
                    wb.statusmessage =
                        ModuleConfig.StatusMessageConfig.Builder().also { wb -> wb.node_status = "Active" }.build()
                }
                .build()
        every { nodeManager.myNodeNum } returns MutableStateFlow<Int?>(myNum)
        everySuspend { nodeManager.updateNodeStatusAndPersist(myNum, "Active") } calls
            {
                statusStarted.complete(Unit)
                releaseStatus.await()
                throw CancellationException("forced node-status cancellation")
            }

        handler.handleModuleConfig(config, session)
        statusStarted.await()
        val persistenceJob = parentJob.children.first { it !in existingJobs }
        releaseStatus.complete(Unit)
        advanceUntilIdle()

        assertTrue(persistenceJob.isCancelled, "node-status cancellation must remain cancellation")
        verifySuspend(mode = VerifyMode.exactly(1)) { radioConfigRepository.setLocalModuleConfig(config) }
        verifySuspend(mode = VerifyMode.exactly(1)) { nodeManager.updateNodeStatusAndPersist(myNum, "Active") }
        parentJob.cancel()
    }

    private fun loraConfig(channelNum: Int) = Config.Builder()
        .also { wb ->
            wb.lora =
                Config.LoRaConfig.Builder()
                    .also {
                        it.use_preset = true
                        it.modem_preset = Config.LoRaConfig.ModemPreset.LONG_FAST
                        it.channel_num = channelNum
                    }
                    .build()
        }
        .build()

    @Test
    fun `handleDeviceConfig forgets the node sync when the LoRa slot moved since the last connection`() =
        runTest(testDispatcher) {
            handler = createHandler(backgroundScope)
            localConfigFlow.value = LocalConfig.Builder().also { wb -> wb.lora = loraConfig(20).lora }.build()

            handler.handleDeviceConfig(loraConfig(20), session)
            advanceUntilIdle()
            assertEquals(1_800_000_000L, meshPrefs.nodeSyncs[MY_NODE_NUM], "same slot keeps the sync")

            handler.handleDeviceConfig(loraConfig(21), session)
            advanceUntilIdle()
            assertEquals(0L, meshPrefs.nodeSyncs[MY_NODE_NUM])
        }

    private fun primaryChannel(name: String) = Channel.Builder()
        .also { wb ->
            wb.index = 0
            wb.role = Channel.Role.PRIMARY
            wb.settings = ChannelSettings.Builder().also { it.name = name }.build()
        }
        .build()

    @Test
    fun `handleChannel forgets the node sync when the primary channel was renamed`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        channelSetFlow.value =
            ChannelSet.Builder()
                .also { wb -> wb.settings = listOf(primaryChannel("MediumFast").settings!!) }
                .build()

        handler.handleChannel(primaryChannel("MediumFast"), session)
        advanceUntilIdle()
        assertEquals(1_800_000_000L, meshPrefs.nodeSyncs[MY_NODE_NUM], "same name keeps the sync")

        handler.handleChannel(primaryChannel("LongFast"), session)
        advanceUntilIdle()
        assertEquals(0L, meshPrefs.nodeSyncs[MY_NODE_NUM])
    }

    // ---------- handleChannel ----------

    @Test
    fun `handleChannel persists channel settings`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val channel = Channel.Builder().also { wb -> wb.index = 0 }.build()
        handler.handleChannel(channel, session)
        advanceUntilIdle()

        verifySuspend { radioConfigRepository.updateChannelSettings(channel) }
    }

    @Test
    fun `handleChannel shows progress with max channels when myNodeInfo available`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        every { nodeManager.getMyNodeInfo() } returns
            MyNodeInfo(
                myNodeNum = 123,
                hasGPS = false,
                model = null,
                firmwareVersion = null,
                couldUpdate = false,
                shouldUpdate = false,
                currentPacketId = 0L,
                messageTimeoutMsec = 0,
                minAppVersion = 0,
                maxChannels = 8,
                hasWifi = false,
                channelUtilization = 0f,
                airUtilTx = 0f,
                deviceId = null,
            )

        val channel = Channel.Builder().also { wb -> wb.index = 2 }.build()
        handler.handleChannel(channel, session)
        advanceUntilIdle()

        verify { serviceRepository.setConnectionProgress("Channels (3 / 8)") }
    }

    @Test
    fun `handleChannel shows progress without max channels when myNodeInfo unavailable`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        every { nodeManager.getMyNodeInfo() } returns null

        val channel = Channel.Builder().also { wb -> wb.index = 0 }.build()
        handler.handleChannel(channel, session)
        advanceUntilIdle()

        verify { serviceRepository.setConnectionProgress("Channels (1)") }
    }

    // ---------- handleDeviceUIConfig ----------

    @Test
    fun `handleDeviceUIConfig persists config`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config = DeviceUIConfig.Builder().build()
        handler.handleDeviceUIConfig(config, session)
        advanceUntilIdle()

        verifySuspend { radioConfigRepository.setDeviceUIConfig(config) }
    }

    // ---------- onHandshakeProgress ----------

    @Test
    fun `handleDeviceConfig calls onHandshakeProgress`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            Config.Builder()
                .also { wb ->
                    wb.device =
                        Config.DeviceConfig.Builder()
                            .also { wb -> wb.role = Config.DeviceConfig.Role.CLIENT }
                            .build()
                }
                .build()
        handler.handleDeviceConfig(config, session)
        advanceUntilIdle()

        verify { connectionManager.onHandshakeProgress() }
    }

    @Test
    fun `handleModuleConfig calls onHandshakeProgress`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val config =
            ModuleConfig.Builder()
                .also { wb -> wb.mqtt = ModuleConfig.MQTTConfig.Builder().also { wb -> wb.enabled = true }.build() }
                .build()
        handler.handleModuleConfig(config, session)
        advanceUntilIdle()

        verify { connectionManager.onHandshakeProgress() }
    }

    @Test
    fun `handleChannel calls onHandshakeProgress`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        every { nodeManager.getMyNodeInfo() } returns null

        val channel = Channel.Builder().also { wb -> wb.index = 0 }.build()
        handler.handleChannel(channel, session)
        advanceUntilIdle()

        verify { connectionManager.onHandshakeProgress() }
    }

    @Test
    fun `handleDeviceUIConfig calls onHandshakeProgress`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        handler.handleDeviceUIConfig(DeviceUIConfig.Builder().build(), session)
        advanceUntilIdle()

        verify { connectionManager.onHandshakeProgress() }
    }

    // ---------- handleRegionPresets ----------

    @Test
    fun `handleRegionPresets persists map`() = runTest(testDispatcher) {
        handler = createHandler(backgroundScope)
        val map = LoRaRegionPresetMap.Builder().build()
        handler.handleRegionPresets(map, session)
        advanceUntilIdle()

        verifySuspend { radioConfigRepository.setLoraRegionPresetMap(map) }
        verify { connectionManager.onHandshakeProgress() }
    }
}
