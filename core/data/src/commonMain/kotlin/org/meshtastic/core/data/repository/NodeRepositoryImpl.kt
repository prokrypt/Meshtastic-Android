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
package org.meshtastic.core.data.repository

import androidx.lifecycle.Lifecycle
import androidx.lifecycle.coroutineScope
import androidx.lifecycle.repeatOnLifecycle
import co.touchlab.kermit.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.koin.core.annotation.Named
import org.koin.core.annotation.Single
import org.meshtastic.core.common.di.PROCESS_LIFECYCLE
import org.meshtastic.core.common.util.handledLaunch
import org.meshtastic.core.common.util.safeCatching
import org.meshtastic.core.data.datasource.NodeInfoReadDataSource
import org.meshtastic.core.data.datasource.NodeInfoWriteDataSource
import org.meshtastic.core.database.entity.MetadataEntity
import org.meshtastic.core.database.entity.MyNodeEntity
import org.meshtastic.core.database.entity.NodeEntity
import org.meshtastic.core.database.entity.NodeWithRelations
import org.meshtastic.core.database.entity.toEntity
import org.meshtastic.core.datastore.LocalStatsDataSource
import org.meshtastic.core.di.CoroutineDispatchers
import org.meshtastic.core.model.MeshLog
import org.meshtastic.core.model.MyNodeInfo
import org.meshtastic.core.model.Node
import org.meshtastic.core.model.NodeAddress
import org.meshtastic.core.model.NodeSortOption
import org.meshtastic.core.model.matchesSearch
import org.meshtastic.core.model.util.onlineTimeThreshold
import org.meshtastic.core.repository.NodeRepository
import org.meshtastic.proto.DeviceMetadata
import org.meshtastic.proto.LocalStats
import org.meshtastic.proto.User
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Repository for managing node-related data, including hardware info, node database, and identity. */
@Single
@Suppress("TooManyFunctions")
class NodeRepositoryImpl(
    @Named(PROCESS_LIFECYCLE) private val processLifecycle: Lifecycle,
    private val nodeInfoReadDataSource: NodeInfoReadDataSource,
    private val nodeInfoWriteDataSource: NodeInfoWriteDataSource,
    private val dispatchers: CoroutineDispatchers,
    private val localStatsDataSource: LocalStatsDataSource,
) : NodeRepository {
    /** Hardware info about our local device (can be null if not connected). */
    override val myNodeInfo: StateFlow<MyNodeInfo?> =
        nodeInfoReadDataSource
            .myNodeInfoFlow()
            .map { it?.toMyNodeInfo() }
            .flowOn(dispatchers.io)
            .stateIn(processLifecycle.coroutineScope, SharingStarted.Eagerly, null)

    private val _ourNodeInfo = MutableStateFlow<Node?>(null)

    /** Information about the locally connected node, as seen from the mesh. */
    override val ourNodeInfo: StateFlow<Node?>
        get() = _ourNodeInfo

    private val _myId = MutableStateFlow<String?>(null)

    /** The unique userId (hex string) of our local node. */
    override val myId: StateFlow<String?>
        get() = _myId

    /** The newest local stats received in this process, ahead of the saved copy while its save is held back. */
    private val receivedLocalStats = MutableStateFlow<LocalStats?>(null)

    /**
     * Nodes whose database write is held back, by number; their readings are shown over the row (see
     * [showUnsavedNode]).
     */
    private val unsavedNodes = MutableStateFlow<Map<Int, Node>>(emptyMap())

    /** The latest local stats telemetry received from the locally connected node. */
    override val localStats: StateFlow<LocalStats> =
        combine(localStatsDataSource.localStatsFlow, receivedLocalStats) { saved, received -> received ?: saved }
            .stateIn(processLifecycle.coroutineScope, SharingStarted.Eagerly, EMPTY_LOCAL_STATS)

    init {
        // The radio reports local stats about once a minute, and the saved copy is only read back at a cold start,
        // before the radio reports again. So save the first report at once, then at most once per
        // LOCAL_STATS_SAVE_INTERVAL with the newest value. A clear (on disconnect) is saved at once, so a restart never
        // shows the previous radio's stats.
        processLifecycle.coroutineScope.launch {
            receivedLocalStats.filterNotNull().collect { stats ->
                safeCatching { localStatsDataSource.setLocalStats(stats) }
                    .onFailure { Logger.w(it) { "Could not save local stats" } }
                withTimeoutOrNull(LOCAL_STATS_SAVE_INTERVAL) {
                    receivedLocalStats.first { it == EMPTY_LOCAL_STATS }
                }
            }
        }
    }

    /** Update the cached local stats telemetry. Readers see it at once; the saved copy follows as described above. */
    override fun updateLocalStats(stats: LocalStats) {
        receivedLocalStats.value = stats
    }

    /** The rows as stored, before unsaved readings are laid over them. */
    private val savedNodeDB: StateFlow<Map<Int, Node>> =
        nodeInfoReadDataSource
            .nodeDBbyNumFlow()
            .toNodeModels()
            .flowOn(dispatchers.io)
            .conflate()
            .stateIn(processLifecycle.coroutineScope, SharingStarted.Eagerly, emptyMap())

    /**
     * A reactive map from nodeNum to [Node] objects, representing the entire mesh. Readings whose write is held back
     * (see [showUnsavedNode]) are shown over their stored rows.
     *
     * `SharingStarted.Eagerly` over the process lifecycle means a terminal upstream failure is unrecoverable for the
     * process — re-navigation cannot restart the sharing coroutine. [NodeInfoReadDataSource] therefore restarts its DB
     * flows after a recoverable Room pool failure, so this upstream never terminates on a pool wedge (#6608).
     */
    override val nodeDBbyNum: StateFlow<Map<Int, Node>> =
        combine(savedNodeDB, unsavedNodes) { saved, unsaved -> saved.withUnsavedReadings(unsaved) }
            .flowOn(dispatchers.default)
            .conflate()
            .stateIn(processLifecycle.coroutineScope, SharingStarted.Eagerly, emptyMap())

    override fun showUnsavedNode(node: Node) {
        unsavedNodes.update { it + (node.num to node) }
    }

    override fun forgetUnsavedNode(written: Node) {
        // The value shown now is the written one or older; one shown after this call is newer and stays.
        val shownAtWrite = unsavedNodes.value[written.num] ?: return
        processLifecycle.coroutineScope.launch {
            withTimeoutOrNull(UNSAVED_HANDOVER_TIMEOUT) {
                savedNodeDB.first { saved -> saved[written.num]?.let { it.withReadingsFrom(written) == it } ?: true }
            }
            unsavedNodes.update { unsaved ->
                if (unsaved[written.num] === shownAtWrite) unsaved - written.num else unsaved
            }
        }
    }

    override fun forgetUnsavedNodes() {
        unsavedNodes.value = emptyMap()
    }

    override fun canDeferWrite(node: Node): Boolean {
        val saved = savedNodeDB.value[node.num] ?: return false
        // A stored row past the online window changes onlyOnline lists and counts when it comes back, and a change of
        // direct reach changes onlyDirect lists; both are written at once so those SQL queries stay right.
        val keepsListPlacement =
            saved.lastHeard > onlineTimeThreshold() && (saved.hopsAway == 0) == (node.hopsAway == 0)
        val next = node.toEntity()
        return keepsListPlacement && saved.toEntity().withReadingsFrom(next) == next
    }

    override suspend fun upsertAll(nodes: List<Node>) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.upsertAll(nodes.map { it.toEntity() }) }

    /**
     * Maps each row to a [Node], reusing the previous [Node] for a row that is the same instance as last emission.
     * [NodeInfoReadDataSource.nodeDBbyNumFlow] hands back the same instance for every row that did not change, so a
     * packet from one node maps one row instead of the whole mesh.
     */
    private fun Flow<Map<Int, NodeWithRelations>>.toNodeModels(): Flow<Map<Int, Node>> = flow {
        var lastRows: Map<Int, NodeWithRelations> = emptyMap()
        var lastModels: Map<Int, Node> = emptyMap()
        emitAll(
            mapLatest { rows ->
                val models = LinkedHashMap<Int, Node>(rows.size)
                for ((num, row) in rows) {
                    val reused = if (lastRows[num] === row) lastModels[num] else null
                    models[num] = reused ?: row.toModel()
                }
                lastRows = rows
                lastModels = models
                models
            },
        )
    }

    init {
        // Backfill denormalized name columns for existing nodes on startup. A withDb timeout or SQL failure here is
        // reported, not fatal: this scope has no exception handler.
        processLifecycle.coroutineScope.handledLaunch {
            processLifecycle.repeatOnLifecycle(Lifecycle.State.CREATED) {
                withContext(dispatchers.io) { nodeInfoWriteDataSource.backfillDenormalizedNames() }
            }
        }

        // Keep ourNodeInfo and myId correctly updated based on current connection and node DB
        combine(nodeDBbyNum, nodeInfoReadDataSource.myNodeInfoFlow()) { db, info -> info?.myNodeNum?.let { db[it] } }
            .onEach { node ->
                _ourNodeInfo.value = node
                _myId.value = node?.user?.id
            }
            .launchIn(processLifecycle.coroutineScope)
    }

    /**
     * Returns the node number used for log queries. Maps [nodeNum] to [MeshLog.NODE_NUM_LOCAL] (0) if it is the locally
     * connected node.
     */
    override fun effectiveLogNodeId(nodeNum: Int): Flow<Int> = nodeInfoReadDataSource
        .myNodeInfoFlow()
        .map { info -> if (nodeNum == info?.myNodeNum) MeshLog.NODE_NUM_LOCAL else nodeNum }
        .distinctUntilChanged()

    /** Returns the [Node] associated with a given [userId]. Falls back to a generic node if not found. */
    override fun getNode(userId: String): Node = nodeDBbyNum.value.values.find { it.user.id == userId }
        ?: Node(num = NodeAddress.idToNum(userId) ?: 0, user = getUser(userId))

    /** Returns the [User] info for a given [nodeNum]. */
    override fun getUser(nodeNum: Int): User = getUser(NodeAddress.numToDefaultId(nodeNum))

    private val last4 = 4

    /** Returns the [User] info for a given [userId]. Falls back to a generic user if not found. */
    override fun getUser(userId: String): User {
        val found = nodeDBbyNum.value.values.find { it.user.id == userId }?.user
        if (found != null && found.long_name.isNotBlank() && found.short_name.isNotBlank()) {
            return found
        }

        val fallbackId = userId.takeLast(last4)
        // Single equality check replaces two NodeAddress.fromString calls — getUser is called per paged contact
        // and per text-message arrival, so the parser allocations add up.
        val isLocal = userId == NodeAddress.ID_LOCAL
        val defaultLong =
            if (isLocal) {
                ourNodeInfo.value?.user?.long_name?.takeIf { it.isNotBlank() } ?: "Local"
            } else {
                "Meshtastic $fallbackId"
            }
        val defaultShort =
            if (isLocal) {
                ourNodeInfo.value?.user?.short_name?.takeIf { it.isNotBlank() } ?: "Local"
            } else {
                fallbackId
            }

        return found
            ?.newBuilder()
            ?.also { wb ->
                wb.long_name = found.long_name.takeIf { it.isNotBlank() } ?: defaultLong
                wb.short_name = found.short_name.takeIf { it.isNotBlank() } ?: defaultShort
            }
            ?.build()
            ?: User.Builder()
                .also { wb ->
                    wb.id = userId
                    wb.long_name = defaultLong
                    wb.short_name = defaultShort
                }
                .build()
    }

    /** Returns a flow of nodes filtered and sorted according to the parameters. */
    override fun getNodes(
        sort: NodeSortOption,
        filter: String,
        includeUnknown: Boolean,
        onlyOnline: Boolean,
        onlyDirect: Boolean,
    ): Flow<List<Node>> = nodeInfoReadDataSource
        .getNodesFlow(
            sort = sort.sqlValue,
            includeUnknown = includeUnknown,
            onlyDirect = onlyDirect,
            lastHeardMin = if (onlyOnline) onlineTimeThreshold() else -1,
        )
        .combine(unsavedNodes) { rows, unsaved ->
            rows
                .map { row -> row.toModel().let { node -> unsaved[node.num]?.let(node::withReadingsFrom) ?: node } }
                .filter { node -> node.matchesSearch(filter) }
        }
        .flowOn(dispatchers.io)
        .conflate()

    /** Upserts a [Node] to the database. */
    override suspend fun upsert(node: Node) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.upsert(node.toEntity()) }

    /** Installs initial configuration data (local info and remote nodes) into the database. */
    override suspend fun installConfig(mi: MyNodeInfo, nodes: List<Node>): List<Int> = withContext(dispatchers.io) {
        nodeInfoWriteDataSource.installConfig(mi.toEntity(), nodes.map { it.toEntity() })
    }

    /** Deletes all nodes from the database, optionally preserving favorites. */
    override suspend fun clearNodeDB(preserveFavorites: Boolean) = withContext(dispatchers.io) {
        forgetUnsavedNodes()
        nodeInfoWriteDataSource.clearNodeDB(preserveFavorites)
    }

    /** Clears the local node's connection info. */
    override suspend fun clearMyNodeInfo() = withContext(dispatchers.io) { nodeInfoWriteDataSource.clearMyNodeInfo() }

    /** Deletes a node and its metadata by [num]. */
    override suspend fun deleteNode(num: Int) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.deleteNodeAndMetadata(num) }

    /** Deletes multiple nodes and their metadata. */
    override suspend fun deleteNodes(nodeNums: List<Int>) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.deleteNodesAndMetadata(nodeNums) }

    override suspend fun getNodesOlderThan(lastHeard: Int): List<Node> =
        withContext(dispatchers.io) { nodeInfoReadDataSource.getNodesOlderThan(lastHeard).map { it.toModel() } }

    override suspend fun getUnknownNodes(): List<Node> =
        withContext(dispatchers.io) { nodeInfoReadDataSource.getUnknownNodes().map { it.toModel() } }

    override suspend fun getNodeDbSnapshot(): Map<Int, Node> =
        withContext(dispatchers.io) { nodeInfoReadDataSource.getNodeDbSnapshot().mapValues { (_, it) -> it.toModel() } }

    /** Persists hardware metadata for a node. */
    override suspend fun insertMetadata(nodeNum: Int, metadata: DeviceMetadata) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.upsert(MetadataEntity(nodeNum, metadata)) }

    /** Flow emitting the count of nodes currently considered "online". */
    override val onlineNodeCount: Flow<Int> =
        nodeInfoReadDataSource
            .nodeDBbyNumFlow()
            .mapLatest { map -> map.values.count { it.node.lastHeard > onlineTimeThreshold() } }
            .flowOn(dispatchers.io)
            .conflate()

    /** Flow emitting the total number of nodes in the database. */
    override val totalNodeCount: Flow<Int> =
        nodeInfoReadDataSource
            .nodeDBbyNumFlow()
            .mapLatest { map -> map.values.count() }
            .flowOn(dispatchers.io)
            .conflate()

    override suspend fun setNodeNotes(num: Int, notes: String) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.setNodeNotes(num, notes) }

    override suspend fun setNodeDisplayName(num: Int, displayName: String) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.setNodeDisplayName(num, displayName) }

    override suspend fun markAllHeardOnCurrentLora() =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.markAllHeardOnCurrentLora() }

    override suspend fun updatePowerChannelLabel(num: Int, channelIndex: Int, label: String) =
        withContext(dispatchers.io) { nodeInfoWriteDataSource.updatePowerChannelLabel(num, channelIndex, label) }

    private fun MyNodeInfo.toEntity() = MyNodeEntity(
        myNodeNum = myNodeNum,
        model = model,
        firmwareVersion = firmwareVersion,
        couldUpdate = couldUpdate,
        shouldUpdate = shouldUpdate,
        currentPacketId = currentPacketId,
        messageTimeoutMsec = messageTimeoutMsec,
        minAppVersion = minAppVersion,
        maxChannels = maxChannels,
        hasWifi = hasWifi,
        deviceId = deviceId,
        pioEnv = pioEnv,
    )

    private companion object {
        val EMPTY_LOCAL_STATS: LocalStats = LocalStats.Builder().build()

        /** How long a newer local stats report may wait in memory before it is saved. */
        val LOCAL_STATS_SAVE_INTERVAL = 10.minutes

        /** Longest an unsaved value stays shown after its write, waiting for the stored row to catch up. */
        val UNSAVED_HANDOVER_TIMEOUT = 5.seconds
    }
}

/** Lays the held-back readings of [unsaved] over the stored rows they belong to; rows without one stay as stored. */
private fun Map<Int, Node>.withUnsavedReadings(unsaved: Map<Int, Node>): Map<Int, Node> {
    if (unsaved.isEmpty()) return this
    val shown = LinkedHashMap(this)
    for ((num, node) in unsaved) shown[num]?.let { stored -> shown[num] = stored.withReadingsFrom(node) }
    return shown
}

/**
 * The readings a node reports over the mesh, the only fields a deferred write may change (see
 * [NodeRepository.canDeferWrite]). Everything else — identity, keys, names, flags, notes — stays as stored.
 */
private fun Node.withReadingsFrom(o: Node): Node = copy(
    position = o.position,
    snr = o.snr,
    rssi = o.rssi,
    lastHeard = o.lastHeard,
    deviceMetrics = o.deviceMetrics,
    channel = o.channel,
    viaMqtt = o.viaMqtt,
    hopsAway = o.hopsAway,
    environmentMetrics = o.environmentMetrics,
    powerMetrics = o.powerMetrics,
    airQualityMetrics = o.airQualityMetrics,
    soilWaterMetrics = o.soilWaterMetrics,
    paxcounter = o.paxcounter,
    nodeStatus = o.nodeStatus,
    lastTransport = o.lastTransport,
)

private fun NodeEntity.withReadingsFrom(o: NodeEntity): NodeEntity = copy(
    position = o.position,
    latitude = o.latitude,
    longitude = o.longitude,
    snr = o.snr,
    rssi = o.rssi,
    lastHeard = o.lastHeard,
    deviceTelemetry = o.deviceTelemetry,
    channel = o.channel,
    viaMqtt = o.viaMqtt,
    hopsAway = o.hopsAway,
    environmentTelemetry = o.environmentTelemetry,
    powerTelemetry = o.powerTelemetry,
    airQualityTelemetry = o.airQualityTelemetry,
    soilWaterTelemetry = o.soilWaterTelemetry,
    paxcounter = o.paxcounter,
    nodeStatus = o.nodeStatus,
    lastTransport = o.lastTransport,
)
