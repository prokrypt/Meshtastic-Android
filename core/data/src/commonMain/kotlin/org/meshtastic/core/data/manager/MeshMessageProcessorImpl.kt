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

import co.touchlab.kermit.Logger
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update
import kotlinx.collections.immutable.persistentListOf
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.koin.core.annotation.Single
import org.meshtastic.core.common.di.ServiceScope
import org.meshtastic.core.common.util.clampTimestampToNow
import org.meshtastic.core.common.util.handledLaunch
import org.meshtastic.core.common.util.nowMillis
import org.meshtastic.core.common.util.nowSeconds
import org.meshtastic.core.common.util.safeCatching
import org.meshtastic.core.model.MeshLog
import org.meshtastic.core.model.Node
import org.meshtastic.core.model.util.isLora
import org.meshtastic.core.model.util.rxTimeOrNull
import org.meshtastic.core.model.util.snrOrNull
import org.meshtastic.core.model.util.toOneLineString
import org.meshtastic.core.model.util.toPIIString
import org.meshtastic.core.repository.FromRadioPacketHandler
import org.meshtastic.core.repository.MeshDataHandler
import org.meshtastic.core.repository.MeshLogRepository
import org.meshtastic.core.repository.MeshMessageProcessor
import org.meshtastic.core.repository.NodeManager
import org.meshtastic.core.repository.RadioInterfaceService
import org.meshtastic.core.repository.RadioSessionContext
import org.meshtastic.core.repository.ReceivedRadioFrame
import org.meshtastic.core.repository.ServiceStateWriter
import org.meshtastic.core.repository.StorageWriteDeferral
import org.meshtastic.proto.FromRadio
import org.meshtastic.proto.LogRecord
import org.meshtastic.proto.MeshPacket
import org.meshtastic.proto.PortNum
import kotlin.concurrent.Volatile
import kotlin.uuid.Uuid

/** Implementation of [MeshMessageProcessor] that handles raw radio messages and prepares mesh packets for routing. */
@Suppress("TooManyFunctions", "LongParameterList")
@Single
class MeshMessageProcessorImpl(
    private val nodeManager: NodeManager,
    private val serviceStateWriter: ServiceStateWriter,
    private val meshLogRepository: Lazy<MeshLogRepository>,
    private val dataHandler: Lazy<MeshDataHandler>,
    private val fromRadioDispatcher: FromRadioPacketHandler,
    private val radioInterfaceService: RadioInterfaceService,
    private val scope: ServiceScope,
    private val writeDeferral: StorageWriteDeferral,
) : MeshMessageProcessor {

    private data class HeldLog(val log: MeshLog, val session: RadioSessionContext)

    // Mesh log rows held while the app is out of view (see StorageWriteDeferral), oldest first.
    private val heldLogs = atomic(persistentListOf<HeldLog>())

    // When the oldest held row was held; Long.MIN_VALUE while nothing is held.
    private val logsHeldSinceMs = atomic(Long.MIN_VALUE)

    /**
     * Epoch-millisecond timestamp of the last local-node `lastHeard` refresh. Used to throttle refreshes, and the
     * database writes they schedule, to at most once per [LOCAL_NODE_REFRESH_INTERVAL_MS] so that high-frequency
     * FromRadio variants (log records, queue status) don't flood the DB.
     */
    @Volatile private var lastLocalNodeRefreshMs = 0L

    @Volatile private var lastLocalNodeRefreshGeneration = Long.MIN_VALUE

    private data class BufferedMeshPacket(val packet: MeshPacket, val session: RadioSessionContext)

    private val earlyMutex = Mutex()
    private val earlyFlushMutex = Mutex()
    private val earlyReceivedPackets = ArrayDeque<BufferedMeshPacket>()
    private val maxEarlyPacketBuffer = 10240

    override suspend fun clearEarlyPackets() {
        earlyFlushMutex.withLock { earlyMutex.withLock { earlyReceivedPackets.clear() } }
    }

    init {
        // Save held log rows while the ending session can still take a lease and reach its own database.
        radioInterfaceService.addSessionEndingHook { flushDeferredWrites() }
        writeDeferral.addFlusher(::flushDeferredWrites)
        // Flush buffered packets only once BOTH the node DB is ready AND our own node number is known. Processing a
        // received packet while myNodeNum is still null would key a local packet under its raw from_num instead of
        // NODE_NUM_LOCAL (see [handleReceivedMeshPacket] / [processReceivedMeshPacket]), orphaning it from per-node
        // queries. Emit the (ready, myNodeNum) pair — not the derived boolean — so distinctUntilChanged re-fires on
        // every underlying state change (including a reconnect where myNodeNum transitions null -> value) rather than
        // collapsing distinct states that happen to map to the same boolean.
        combine(nodeManager.isNodeDbReady, nodeManager.myNodeNum) { ready, myNodeNum -> ready to myNodeNum }
            .distinctUntilChanged()
            .onEach { (ready, myNodeNum) ->
                if (ready && myNodeNum != null) {
                    flushEarlyReceivedPackets("ready")
                }
            }
            .launchIn(scope)
    }

    override suspend fun handleFromRadio(frame: ReceivedRadioFrame, myNodeNum: Int?) {
        if (!radioInterfaceService.isSessionActive(frame.session)) {
            Logger.d { "Dropping decoded work from stale transport session gen=${frame.session.generation}" }
            return
        }
        val bytes = frame.payload.toByteArray()
        val proto = safeCatching {
            FromRadio.ADAPTER.decode(bytes)
        }
            .getOrElse { primaryException ->
                safeCatching {
                    FromRadio.Builder().also { wb -> wb.log_record = LogRecord.ADAPTER.decode(bytes) }.build()
                }
                    .getOrElse {
                        Logger.e(primaryException) {
                            "Failed to parse radio packet (len=${bytes.size}). Not a valid FromRadio or LogRecord."
                        }
                        return
                    }
            }
        processFromRadio(proto, myNodeNum, frame.session)
    }

    private suspend fun processFromRadio(proto: FromRadio, myNodeNum: Int?, session: RadioSessionContext) {
        val admitted =
            radioInterfaceService.runWhileSessionActive(session, "FromRadio ${proto.variantLabel()}") {
                safeCatching {
                    // Audit log every incoming variant without allowing delayed work to cross a session boundary.
                    logVariant(proto, session)

                    val packet = proto.packet
                    if (packet != null) {
                        handleReceivedMeshPacket(packet, myNodeNum, session)
                    } else {
                        // Packets refresh the local node in processReceivedMeshPacket; other variants need the
                        // heartbeat.
                        refreshLocalNodeLastHeard(session)
                        fromRadioDispatcher.handleFromRadio(proto, session)
                    }
                }
                    .onFailure {
                        Logger.e(it) { "Dropped a FromRadio after a handler error; receive pipeline kept alive" }
                    }
            }
        if (!admitted) {
            Logger.d { "Dropping FromRadio from stale transport session gen=${session.generation}" }
        }
    }

    private fun logVariant(proto: FromRadio, session: RadioSessionContext) {
        val myInfo = proto.my_info
        val nodeInfo = proto.node_info
        val config = proto.config
        val moduleConfig = proto.moduleConfig
        val channel = proto.channel
        val (type, message) =
            when {
                proto.log_record != null -> "LogRecord" to proto.log_record.toString()
                proto.rebooted != null -> "Rebooted" to proto.rebooted.toString()
                proto.xmodemPacket != null -> "XmodemPacket" to proto.xmodemPacket.toString()
                proto.deviceuiConfig != null -> "DeviceUIConfig" to proto.deviceuiConfig.toString()
                proto.fileInfo != null -> "FileInfo" to proto.fileInfo.toString()
                myInfo != null -> "MyInfo" to myInfo.toOneLineString()
                nodeInfo != null -> "NodeInfo" to nodeInfo.toPIIString()
                config != null -> "Config" to config.toOneLineString()
                moduleConfig != null -> "ModuleConfig" to moduleConfig.toOneLineString()
                channel != null -> "Channel" to channel.toOneLineString()
                proto.clientNotification != null -> "ClientNotification" to proto.clientNotification.toString()
                else -> return
            }

        insertMeshLog(
            MeshLog(
                uuid = Uuid.random().toString(),
                message_type = type,
                received_date = nowMillis,
                raw_message = message,
                fromRadio = proto,
            ),
            session,
        )
    }

    /** Test seam for packet-only fixtures with explicit transport authority. */
    internal suspend fun handleReceivedMeshPacket(packet: MeshPacket, myNodeNum: Int?, session: RadioSessionContext) {
        // Single normalization point: every consumer downstream of this copy sees a stamped packet.
        val preparedPacket =
            packet.newBuilder().also { wb -> wb.rx_time = packet.rxTimeOrNull() ?: nowSeconds.toInt() }.build()

        // Require myNodeNum to be known before storing: processReceivedMeshPacket only keys a local packet under
        // NODE_NUM_LOCAL when packet.from == myNodeNum. If myNodeNum is still null (early in a (re)connect, before
        // MyNodeInfo resolves), a local packet would be stored under its raw from_num and become invisible to
        // per-node chart queries while still appearing in the unfiltered Debug log. Buffer until both are ready;
        // the init combine flushes the buffer once myNodeNum resolves.
        if (!radioInterfaceService.isSessionActive(session)) {
            Logger.d { "Dropping mesh packet from stale transport session gen=${session.generation}" }
            return
        }

        if (nodeManager.isNodeDbReady.value && myNodeNum != null) {
            processReceivedMeshPacket(preparedPacket, myNodeNum, session)
        } else {
            // Production callers already hold the session lease in processFromRadio. Enqueue directly so packet FIFO
            // does not depend on the ordering of separately launched mutex waiters.
            enqueueBufferedPacket(BufferedMeshPacket(preparedPacket, session))
            if (nodeManager.isNodeDbReady.value && nodeManager.myNodeNum.value != null) {
                flushEarlyReceivedPackets("enqueue-ready")
            }
        }
    }

    private suspend fun enqueueBufferedPacket(buffered: BufferedMeshPacket) = earlyMutex.withLock {
        discardBufferedPacketsFromOtherSessions(buffered.session)
        val queueSize = earlyReceivedPackets.size
        if (queueSize >= maxEarlyPacketBuffer) {
            Logger.w { "Early packet buffer full ($queueSize), dropping oldest packet" }
            earlyReceivedPackets.removeFirstOrNull()
        }
        earlyReceivedPackets.addLast(buffered)
    }

    private fun discardBufferedPacketsFromOtherSessions(activeSession: RadioSessionContext) {
        val removed = earlyReceivedPackets.removeAll { queued -> queued.session != activeSession }
        if (removed) Logger.d { "Discarded buffered packets from an earlier transport generation" }
    }

    private fun flushEarlyReceivedPackets(reason: String) {
        scope.launch { earlyFlushMutex.withLock { replayEarlyReceivedPackets(reason) } }
    }

    private suspend fun replayEarlyReceivedPackets(reason: String) {
        var replayed = 0
        while (nodeManager.isNodeDbReady.value) {
            val myNodeNum = nodeManager.myNodeNum.value ?: return
            val buffered = earlyMutex.withLock { earlyReceivedPackets.removeFirstOrNull() } ?: break

            // Readiness can regress while a queued flush waits behind an earlier replay. Put the packet back at the
            // front instead of keying it against a database that is being cleared or an unresolved local node number.
            if (!isReplayReady(myNodeNum)) {
                earlyMutex.withLock { earlyReceivedPackets.addFirst(buffered) }
                return
            }

            if (processBufferedPacket(buffered, myNodeNum)) replayed += 1
        }
        if (replayed > 0) Logger.d { "replayEarlyPackets reason=$reason count=$replayed" }
    }

    private fun isReplayReady(myNodeNum: Int): Boolean =
        nodeManager.isNodeDbReady.value && nodeManager.myNodeNum.value == myNodeNum

    private suspend fun processBufferedPacket(buffered: BufferedMeshPacket, myNodeNum: Int): Boolean {
        val admitted =
            radioInterfaceService.runWhileSessionActive(
                buffered.session,
                "buffered packet ${buffered.packet.portLabel()}",
            ) {
                safeCatching { processReceivedMeshPacket(buffered.packet, myNodeNum, buffered.session) }
                    .onFailure {
                        Logger.e(it) { "Dropped a buffered early packet after a handler error; replay continued" }
                    }
            }
        if (!admitted) {
            Logger.d { "Dropping buffered packet from stale transport session gen=${buffered.session.generation}" }
        }
        return admitted
    }

    @Suppress("LongMethod")
    private suspend fun processReceivedMeshPacket(packet: MeshPacket, myNodeNum: Int, session: RadioSessionContext) {
        val decoded = packet.decoded ?: return
        val log =
            MeshLog(
                uuid = Uuid.random().toString(),
                message_type = "Packet",
                received_date = nowMillis,
                // The text is rebuilt from fromRadio when read; storing it too made each row ~8x larger.
                raw_message = "",
                fromNum = if (packet.from == myNodeNum) MeshLog.NODE_NUM_LOCAL else packet.from,
                portNum = decoded.portnum.value,
                fromRadio = FromRadio.Builder().also { wb -> wb.packet = packet }.build(),
            )
        val logJob = insertMeshLog(log, session)

        launchSessionBound(session, "mesh-packet emission") { serviceStateWriter.emitMeshPacket(packet) }

        // The in-memory update is synchronous; the database write runs on its own session lease so this handler,
        // which holds the whole inbound pipeline, never waits on the database writer gate. The batch makes the sender
        // one write per packet: a position or telemetry handler below updates the same node again.
        nodeManager.beginNodeWriteBatch()
        try {
            val from = packet.from
            if (from == myNodeNum) {
                nodeManager.updateNodeForSession(myNodeNum, session, channel = packet.channel) { node ->
                    applySenderPacketUpdate(node, packet, decoded).copy(lastHeard = nowSeconds.toInt())
                }
            } else {
                // Any frame proves the link is alive, so the local node shares the heartbeat's throttled refresh,
                // which updates the screen every 30 s but writes the row at most every 10 minutes.
                refreshLocalNodeLastHeard(session, myNodeNum)
                nodeManager.updateNodeForSession(from, session, channel = packet.channel) { node ->
                    applySenderPacketUpdate(node, packet, decoded)
                }
            }

            dataHandler.value.handleReceivedData(packet, myNodeNum, session, log.uuid, logJob)
        } finally {
            nodeManager.endNodeWriteBatch()
        }
    }

    private fun applySenderPacketUpdate(node: Node, packet: MeshPacket, decoded: org.meshtastic.proto.Data): Node {
        val viaMqtt = packet.via_mqtt == true
        val isDirect = packet.hop_start == packet.hop_limit
        val updateRadioMetrics = isDirect && packet.isLora() && !viaMqtt
        // Every real reception carries rx_rssi; the stored packets firmware replays after a node DB dump are marked
        // LoRa
        // but carry none, and were heard earlier, maybe on other settings.
        val heardOverOurRadio = packet.rx_rssi != null && !viaMqtt
        val hopsAway =
            when {
                decoded.portnum == PortNum.RANGE_TEST_APP -> 0
                viaMqtt -> -1
                packet.hop_start == 0 && (decoded.bitfield ?: 0) == 0 -> -1
                packet.hop_limit > packet.hop_start -> -1
                else -> packet.hop_start - packet.hop_limit
            }
        return node.copy(
            // Packets reach here already stamped, so the fallback is unreachable by design and must stay that way:
            // a packet that just arrived SHOULD refresh lastHeard, using the phone's clock when the radio had no
            // time source. The fallback only guards a future caller that skips normalization from writing the epoch.
            lastHeard = packet.rxTimeOrNull()?.let(::clampTimestampToNow) ?: node.lastHeard,
            viaMqtt = viaMqtt,
            heardOnCurrentLora = node.heardOnCurrentLora || heardOverOurRadio,
            lastTransport = packet.transport_mechanism.value,
            // A packet carrying no snr must not clobber the node's last real reading either.
            snr = if (updateRadioMetrics) packet.snrOrNull() ?: node.snr else node.snr,
            // A packet carrying no rssi must not clobber the node's last real reading.
            rssi = if (updateRadioMetrics) packet.rx_rssi ?: node.rssi else node.rssi,
            hopsAway = hopsAway,
        )
    }

    /**
     * Refreshes the local node's [Node.lastHeard] to prove the radio link is alive.
     *
     * Every frame proves it: packets relayed from other nodes, and on a quiet mesh the heartbeat cycle (ToRadio
     * heartbeat → FromRadio queueStatus every 30 s). Without this the local node would look stale in the UI even though
     * the connection is healthy. Packets the local node sends itself update it directly in [processReceivedMeshPacket].
     *
     * Frames come fast (one per relayed packet on a busy mesh, and log records many times per second when debug logging
     * is enabled), so the screen is refreshed at most once per [LOCAL_NODE_REFRESH_INTERVAL_MS], and
     * [NodeManager.refreshLastHeard] writes the row far less often than that. Any write is scheduled on the node's
     * session lease like the packet updates, so the handler never waits on the database.
     */
    private fun refreshLocalNodeLastHeard(session: RadioSessionContext, myNodeNum: Int? = nodeManager.myNodeNum.value) {
        val now = nowMillis
        val sameGeneration = lastLocalNodeRefreshGeneration == session.generation
        if (sameGeneration && now - lastLocalNodeRefreshMs < LOCAL_NODE_REFRESH_INTERVAL_MS) return

        val myNum = myNodeNum ?: return
        nodeManager.refreshLastHeard(myNum, nowSeconds.toInt(), session)
        lastLocalNodeRefreshGeneration = session.generation
        lastLocalNodeRefreshMs = now
    }

    /** Names the variant for diagnostics from field names and the port only, never payload values. */
    private fun FromRadio.variantLabel(): String = packet?.let { "packet ${it.portLabel()}" }
        ?: when {
            my_info != null -> "my_info"
            node_info != null -> "node_info"
            config != null -> "config"
            moduleConfig != null -> "moduleConfig"
            channel != null -> "channel"
            config_complete_id != null -> "config_complete_id"
            metadata != null -> "metadata"
            deviceuiConfig != null -> "deviceuiConfig"
            fileInfo != null -> "fileInfo"
            region_presets != null -> "region_presets"
            queueStatus != null -> "queueStatus"
            log_record != null -> "log_record"
            mqttClientProxyMessage != null -> "mqttClientProxyMessage"
            xmodemPacket != null -> "xmodemPacket"
            lockdown_status != null -> "lockdown_status"
            clientNotification != null -> "clientNotification"
            rebooted != null -> "rebooted"
            else -> "other"
        }

    private fun MeshPacket.portLabel(): String = decoded?.portnum?.name ?: "encrypted"

    /** Returns the write's job, or null when the row is held in memory (see [StorageWriteDeferral]). */
    private fun insertMeshLog(log: MeshLog, session: RadioSessionContext): Job? {
        // A traceroute's row is written at once: its handler waits for it and stores a snapshot that refers to it.
        if (writeDeferral.deferring.value && log.portNum != PortNum.TRACEROUTE_APP.value) {
            holdLog(log, session)
            return null
        }
        return launchSessionBound(session, "mesh-log insert") { meshLogRepository.value.insert(log) }
    }

    private fun holdLog(log: MeshLog, session: RadioSessionContext) {
        var held = 0
        heldLogs.update { logs -> logs.adding(HeldLog(log, session)).also { held = it.size } }
        val now = nowMillis
        logsHeldSinceMs.compareAndSet(Long.MIN_VALUE, now)
        val since = logsHeldSinceMs.value
        val due =
            held >= StorageWriteDeferral.MAX_DEFERRED_LOG_ROWS ||
                now - since >= StorageWriteDeferral.SAFETY_FLUSH_INTERVAL_MS ||
                now < since
        if (due && logsHeldSinceMs.compareAndSet(since, now)) scope.handledLaunch { flushDeferredWrites() }
    }

    override suspend fun flushDeferredWrites() {
        val held = heldLogs.getAndSet(persistentListOf())
        logsHeldSinceMs.value = Long.MIN_VALUE
        if (held.isEmpty()) return
        held
            .groupBy { it.session }
            .forEach { (session, logs) ->
                val admitted =
                    radioInterfaceService.runWithSessionLease(session) {
                        meshLogRepository.value.insertAll(logs.map { row -> row.log })
                    }
                if (!admitted) {
                    Logger.w { "Dropped ${logs.size} held mesh log rows: their radio session ended first" }
                }
            }
    }

    private fun launchSessionBound(session: RadioSessionContext, operation: String, block: suspend () -> Unit): Job =
        scope.handledLaunch(start = CoroutineStart.UNDISPATCHED) {
            val admitted = radioInterfaceService.runWithSessionLease(session) { block() }
            if (!admitted) {
                Logger.d { "Skipping $operation from stale transport session gen=${session.generation}" }
            }
        }

    companion object {
        /**
         * Minimum interval between local-node `lastHeard` refreshes in memory and on screen, in milliseconds. Aligned
         * with the heartbeat interval (30 s). The database write behind them is spaced further apart by
         * [NodeManager.refreshLastHeard].
         */
        private const val LOCAL_NODE_REFRESH_INTERVAL_MS = 30_000L
    }
}
