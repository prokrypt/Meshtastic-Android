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
package org.meshtastic.core.model

import org.meshtastic.proto.Config

/**
 * The LoRa fields firmware's slot fingerprint covers, which decides `NodeInfo.heard_on_current_lora`; the unused half
 * of preset or custom modem moves nothing.
 */
val Config.LoRaConfig.heardSlot: List<Any?>
    get() {
        val modem = if (use_preset) listOf(modem_preset) else listOf(bandwidth, spread_factor, coding_rate)
        return listOf(region, use_preset, override_frequency, channel_num) + modem
    }
