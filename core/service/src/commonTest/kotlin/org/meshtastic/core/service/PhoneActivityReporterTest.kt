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

import dev.mokkery.MockMode
import dev.mokkery.answering.calls
import dev.mokkery.every
import dev.mokkery.matcher.any
import dev.mokkery.mock
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.meshtastic.core.model.ConnectionState
import org.meshtastic.core.repository.LiveUpdateMode
import org.meshtastic.core.repository.PacketHandler
import org.meshtastic.core.repository.StorageWriteDeferral
import org.meshtastic.core.testing.FakeLiveUpdatePrefs
import org.meshtastic.core.testing.FakeServiceRepository
import org.meshtastic.core.testing.FakeTakPrefs
import org.meshtastic.proto.ToRadio
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class PhoneActivityReporterTest {
    private val serviceRepository = FakeServiceRepository()
    private val deferral = StorageWriteDeferral()
    private val liveUpdatePrefs = FakeLiveUpdatePrefs()
    private val takPrefs = FakeTakPrefs()
    private val sent = mutableListOf<ToRadio>()
    private val packetHandler: PacketHandler =
        mock(MockMode.autofill) {
            every { trySendToRadio(any()) } calls
                {
                    sent += it.arg<ToRadio>(0)
                    true
                }
        }

    private fun reporter() =
        DefaultPhoneActivityReporter(serviceRepository, deferral, liveUpdatePrefs, takPrefs, packetHandler)

    /** The idle flag of each marker sent so far, checking each one carries the tag. */
    private fun markers(): List<Boolean> = sent.map { toRadio ->
        val nonce = toRadio.heartbeat!!.nonce
        assertEquals(DefaultPhoneActivityReporter.ACTIVITY_TAG, nonce and TAG_MASK, "marker carries the tag")
        nonce and 1 == 1
    }

    @Test
    fun `nothing is sent before a connection completes`() = runTest(UnconfinedTestDispatcher()) {
        reporter().start(backgroundScope)
        deferral.setAppForeground(false)
        serviceRepository.setConnectionState(ConnectionState.Connecting)

        assertTrue(sent.isEmpty())
    }

    @Test
    fun `the state goes out after the handshake and on every change`() = runTest(UnconfinedTestDispatcher()) {
        reporter().start(backgroundScope)
        deferral.setAppForeground(true)
        serviceRepository.setConnectionState(ConnectionState.Connected)

        deferral.setAppForeground(false)
        deferral.setAppForeground(true)

        assertEquals(listOf(false, true, false), markers())
    }

    @Test
    fun `live update or the TAK server keep the app active`() = runTest(UnconfinedTestDispatcher()) {
        reporter().start(backgroundScope)
        deferral.setAppForeground(false)
        serviceRepository.setConnectionState(ConnectionState.Connected)

        liveUpdatePrefs.setMode(LiveUpdateMode.MeshStatus)
        liveUpdatePrefs.setMode(LiveUpdateMode.Off)
        takPrefs.setTakServerEnabled(true)

        assertEquals(listOf(true, false, true, false), markers())
    }

    @Test
    fun `a reconnect sends the state again`() = runTest(UnconfinedTestDispatcher()) {
        reporter().start(backgroundScope)
        deferral.setAppForeground(false)
        serviceRepository.setConnectionState(ConnectionState.Connected)

        serviceRepository.setConnectionState(ConnectionState.DeviceSleep)
        serviceRepository.setConnectionState(ConnectionState.Connected)

        assertEquals(listOf(true, true), markers())
    }

    @Test
    fun `repeated markers differ so the radio never drops one as a duplicate`() = runTest(UnconfinedTestDispatcher()) {
        reporter().start(backgroundScope)
        deferral.setAppForeground(false)
        serviceRepository.setConnectionState(ConnectionState.Connected)
        serviceRepository.setConnectionState(ConnectionState.Disconnected)
        serviceRepository.setConnectionState(ConnectionState.Connected)

        assertEquals(2, sent.size)
        assertNotEquals(sent[0], sent[1])
    }

    @Test
    fun `the nonce keeps the tag when the counter wraps`() {
        val nonce = DefaultPhoneActivityReporter.activityNonce(counter = Int.MAX_VALUE, idle = true)

        assertEquals(DefaultPhoneActivityReporter.ACTIVITY_TAG, nonce and TAG_MASK)
        assertEquals(1, nonce and 1)
    }

    private companion object {
        const val TAG_MASK = 0xFFFF0000.toInt()
    }
}
