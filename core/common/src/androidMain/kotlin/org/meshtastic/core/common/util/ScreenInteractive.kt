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

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager
import androidx.core.content.ContextCompat
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.distinctUntilChanged

/**
 * Whether the screen is on ([PowerManager.isInteractive]), then each change. Surfaces only seen with the screen on —
 * the notification shade, home-screen widgets — can skip redraws while this is false and catch up when it turns true.
 *
 * The receiver is registered before the current state is read, so a change in between is not missed. Registered
 * not-exported: a protected system broadcast is still delivered (see `AndroidLocaleChangeNotifier`).
 */
fun Context.screenInteractive(): Flow<Boolean> = callbackFlow {
    val receiver =
        object : BroadcastReceiver() {
            override fun onReceive(context: Context?, intent: Intent?) {
                when (intent?.action) {
                    Intent.ACTION_SCREEN_ON -> trySend(true)
                    Intent.ACTION_SCREEN_OFF -> trySend(false)
                }
            }
        }
    val filter =
        IntentFilter().apply {
            addAction(Intent.ACTION_SCREEN_ON)
            addAction(Intent.ACTION_SCREEN_OFF)
        }
    val appContext = applicationContext ?: this@screenInteractive
    ContextCompat.registerReceiver(appContext, receiver, filter, ContextCompat.RECEIVER_NOT_EXPORTED)
    // Unknown counts as on, so a missing PowerManager never hides an update.
    trySend(appContext.getSystemService(PowerManager::class.java)?.isInteractive ?: true)
    awaitClose { appContext.unregisterReceiver(receiver) }
}
    .distinctUntilChanged()
