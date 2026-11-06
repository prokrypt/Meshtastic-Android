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
package org.meshtastic.core.repository

import co.touchlab.kermit.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.meshtastic.core.common.util.safeCatching

/**
 * Decides when routine database writes (node updates and mesh log rows) may wait in memory instead of reaching flash.
 *
 * While no part of the app is visible, those writes are held and saved together when the radio session ends
 * (disconnect, device switch, service stop), when the radio reports it disconnected or went to sleep, when the app
 * comes back into view, when the buffer has waited [SAFETY_FLUSH_INTERVAL_MS], or when it grows past its cap. While the
 * app is visible they are written at once, because its screens read the database. The process lifecycle drives
 * [setAppForeground]; a fresh instance writes through.
 *
 * @param scope runs [flushAll] when the app comes back into view; without one, the holders flush on their own triggers.
 */
class StorageWriteDeferral(private val scope: CoroutineScope? = null) {

    private val _deferring = MutableStateFlow(false)

    /** True while routine writes may be held in memory. */
    val deferring: StateFlow<Boolean> = _deferring.asStateFlow()

    private val flushers = MutableStateFlow<List<suspend () -> Unit>>(emptyList())

    /** Registers a holder's save-everything-now call, run by [flushAll]. */
    fun addFlusher(flush: suspend () -> Unit) {
        flushers.update { it + flush }
    }

    /** Saves every held write now. A failing holder is logged and the rest still run. */
    suspend fun flushAll() {
        flushers.value.forEach { flush ->
            safeCatching { flush() }.onFailure { e -> Logger.e(e) { "Saving held writes failed" } }
        }
    }

    fun setAppForeground(foreground: Boolean) {
        val wasDeferring = _deferring.value
        _deferring.value = !foreground
        if (foreground && wasDeferring) scope?.launch { flushAll() }
    }

    companion object {
        /** Longest a held write waits before it is saved anyway, so a killed app loses at most about this much. */
        const val SAFETY_FLUSH_INTERVAL_MS: Long = 60 * 60_000L

        /** Mesh log rows held at most before they are saved anyway, to bound memory (about 1-2 KB each). */
        const val MAX_DEFERRED_LOG_ROWS: Int = 2_000
    }
}
