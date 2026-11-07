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
package org.meshtastic.core.service

import co.touchlab.kermit.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import org.koin.core.annotation.Single
import org.meshtastic.core.common.util.safeCatching
import org.meshtastic.core.model.ConnectionState
import org.meshtastic.core.repository.LiveUpdateMode
import org.meshtastic.core.repository.LiveUpdatePrefs
import org.meshtastic.core.repository.PacketHandler
import org.meshtastic.core.repository.ServiceRepository
import org.meshtastic.core.repository.StorageWriteDeferral
import org.meshtastic.core.repository.TakPrefs
import org.meshtastic.proto.Heartbeat
import org.meshtastic.proto.ToRadio

/**
 * Tells the radio whether anything in the app is showing what it receives, so firmware that supports it can hold
 * routine readings (positions, node info, telemetry) for up to a minute instead of waking the phone for each one.
 * Messages, acks and anything else go out at once either way.
 */
fun interface PhoneActivityReporter {
    /** Reports for as long as [scope] lives. */
    fun start(scope: CoroutineScope)
}

/**
 * The app counts as idle while none of its screens is visible, Live Update is off (its notification shows readings on
 * the lock screen) and the TAK server is off (TAK clients expect positions as they arrive). The state is sent after
 * every completed handshake, since the radio starts each connection as active, and again on every change.
 *
 * The marker is a heartbeat whose nonce carries [ACTIVITY_TAG] in its top 16 bits and the idle flag in bit 0. Firmware
 * without support treats it as an ordinary heartbeat.
 */
@Single
class DefaultPhoneActivityReporter(
    private val serviceRepository: ServiceRepository,
    private val storageWriteDeferral: StorageWriteDeferral,
    private val liveUpdatePrefs: LiveUpdatePrefs,
    private val takPrefs: TakPrefs,
    private val packetHandler: PacketHandler,
) : PhoneActivityReporter {
    // Varies the low bits so the firmware never drops a marker as a repeat of the previous write.
    private var counter = 0

    override fun start(scope: CoroutineScope) {
        serviceRepository.connectionLifecycle
            .map { lifecycle ->
                lifecycle.epochs.completedHandshakes.takeIf { lifecycle.state is ConnectionState.Connected }
            }
            .distinctUntilChanged()
            .flatMapLatest { handshake -> if (handshake == null) emptyFlow() else idle() }
            .onEach(::report)
            .launchIn(scope)
    }

    private fun idle(): Flow<Boolean> = combine(
        storageWriteDeferral.deferring,
        liveUpdatePrefs.mode,
        takPrefs.isTakServerEnabled,
    ) { outOfView, liveUpdate, takServer ->
        outOfView && liveUpdate == LiveUpdateMode.Off && !takServer
    }
        .distinctUntilChanged()

    private fun report(idle: Boolean) {
        val nonce = activityNonce(counter++, idle)
        val heartbeat =
            ToRadio.Builder()
                .also { wb -> wb.heartbeat = Heartbeat.Builder().also { hb -> hb.nonce = nonce }.build() }
                .build()
        val sent = safeCatching { packetHandler.trySendToRadio(heartbeat) }.getOrDefault(false)
        if (sent) {
            Logger.d { "Told the radio the app is ${if (idle) "idle" else "active"}" }
        } else {
            Logger.d { "Could not tell the radio the app is ${if (idle) "idle" else "active"}" }
        }
    }

    internal companion object {
        const val ACTIVITY_TAG = 0xBA7C0000.toInt()
        private const val COUNTER_MASK = 0x7FFF
        private const val IDLE_BIT = 1

        /** The heartbeat nonce for one marker: tag, a 15-bit counter, then the idle flag. */
        fun activityNonce(counter: Int, idle: Boolean): Int =
            ACTIVITY_TAG or ((counter and COUNTER_MASK) shl 1) or (if (idle) IDLE_BIT else 0)
    }
}
