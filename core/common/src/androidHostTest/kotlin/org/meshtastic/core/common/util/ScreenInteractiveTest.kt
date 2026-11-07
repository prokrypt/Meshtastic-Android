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
package org.meshtastic.core.common.util

import android.content.Intent
import android.os.Looper
import android.os.PowerManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import kotlin.test.assertEquals

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34])
class ScreenInteractiveTest {

    private val context = RuntimeEnvironment.getApplication()

    @Test
    fun `starts from the current screen state and follows the broadcasts`() = runBlocking {
        shadowOf(context.getSystemService(PowerManager::class.java)).setIsInteractive(false)
        val states = Channel<Boolean>(Channel.UNLIMITED)
        val collector = launch(Dispatchers.Unconfined) { context.screenInteractive().collect { states.send(it) } }

        assertEquals(false, states.next())

        broadcast(Intent.ACTION_SCREEN_ON)
        assertEquals(true, states.next())

        // A repeat of the same state is not a change.
        broadcast(Intent.ACTION_SCREEN_ON)
        broadcast(Intent.ACTION_SCREEN_OFF)
        assertEquals(false, states.next())

        collector.cancel()
    }

    private fun broadcast(action: String) {
        context.sendBroadcast(Intent(action))
        shadowOf(Looper.getMainLooper()).idle()
    }

    private suspend fun Channel<Boolean>.next(): Boolean = withTimeout(TIMEOUT_MS) { receive() }

    private companion object {
        const val TIMEOUT_MS = 2_000L
    }
}
