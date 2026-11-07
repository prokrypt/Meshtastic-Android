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
package org.meshtastic.core.network.service

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.http.isSuccess
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.koin.core.annotation.Single
import org.meshtastic.core.model.BootloaderOtaQuirksResponse
import org.meshtastic.core.model.EventFirmwareResponse
import org.meshtastic.core.model.FirmwareReleaseManifest
import org.meshtastic.core.model.MaintenanceUf2Manifest
import org.meshtastic.core.model.MqttRootsResponse
import org.meshtastic.core.model.NetworkDeviceHardware
import org.meshtastic.core.model.NetworkDeviceLinksResponse
import org.meshtastic.core.model.NetworkFirmwareNightly
import org.meshtastic.core.model.NetworkFirmwareRelease
import org.meshtastic.core.model.NetworkFirmwareReleases
import org.meshtastic.core.model.Releases
import org.meshtastic.core.network.HttpClientDefaults

/**
 * Pointer to the nightly preview build published by CI, served at the root of [HttpClientDefaults.NIGHTLY_BASE_URL]
 * rather than by api.meshtastic.org.
 *
 * Decoded manually rather than via content negotiation so that a 404 — nothing currently published — can be told apart
 * from a transport failure. The not-found body is the host's HTML error page, which content negotiation would reject
 * before the status could be inspected.
 */
private const val NIGHTLY_INDEX_URL = "${HttpClientDefaults.NIGHTLY_BASE_URL}/index.json"

/** Fork: the firmware fork's release list (newest first). Drafts are not visible to unauthenticated clients. */
private const val FORK_FIRMWARE_RELEASES_URL =
    "https://api.github.com/repos/${HttpClientDefaults.FORK_FIRMWARE_REPO}/releases?per_page=20"

@Serializable
internal data class GitHubAsset(
    @SerialName("name") val name: String = "",
    @SerialName("browser_download_url") val downloadUrl: String = "",
)

@Serializable
internal data class GitHubRelease(
    @SerialName("tag_name") val tagName: String = "",
    @SerialName("name") val name: String? = null,
    @SerialName("html_url") val htmlUrl: String = "",
    @SerialName("body") val body: String? = null,
    @SerialName("draft") val draft: Boolean = false,
    @SerialName("assets") val assets: List<GitHubAsset> = emptyList(),
)

/**
 * Maps the fork's GitHub releases into the API's release shape. The fork has a single release line, so every published
 * release is offered on both the stable and alpha channels. `zip_url` is the release's `firmware-<version>.json` target
 * manifest, as upstream's API now serves it; a release without one cannot be resolved and is skipped.
 */
internal fun List<GitHubRelease>.asNetworkFirmwareReleases(): NetworkFirmwareReleases {
    val releases =
        filter { !it.draft && it.tagName.isNotBlank() }
            .mapNotNull { release ->
                val manifestName = "firmware-${release.tagName.removePrefix("v")}.json"
                val manifest = release.assets.firstOrNull { it.name == manifestName } ?: return@mapNotNull null
                NetworkFirmwareRelease(
                    id = release.tagName,
                    pageUrl = release.htmlUrl,
                    releaseNotes = release.body.orEmpty(),
                    title = release.name?.takeIf { it.isNotBlank() } ?: release.tagName,
                    zipUrl = manifest.downloadUrl,
                )
            }
    return NetworkFirmwareReleases(Releases(alpha = releases, stable = releases))
}

private val firmwareJson = Json {
    isLenient = true
    ignoreUnknownKeys = true
    coerceInputValues = true
}

/**
 * Decodes a GitHub release manifest independently of Ktor content negotiation. GitHub release assets commonly use
 * `application/octet-stream` even when their payload is JSON.
 */
internal fun decodeFirmwareReleaseManifest(body: String): FirmwareReleaseManifest = firmwareJson.decodeFromString(body)

/** Client for the Meshtastic public API (device hardware catalog and firmware releases). */
interface ApiService {
    /** Fetches the device hardware catalog from the Meshtastic API. */
    suspend fun getDeviceHardware(): List<NetworkDeviceHardware>

    /** Fetches the resolved device-links catalog (msh.to purchase links) from the Meshtastic API. */
    suspend fun getDeviceLinks(): NetworkDeviceLinksResponse

    /** Fetches the list of available firmware releases from the firmware fork's GitHub releases. */
    suspend fun getFirmwareReleases(): NetworkFirmwareReleases

    /** Fetches the target manifest referenced by a firmware release's `zip_url`. */
    suspend fun getFirmwareReleaseManifest(manifestUrl: String): FirmwareReleaseManifest

    /**
     * Fetches the nightly preview build pointer from the nightly host. Returns null when no nightly is currently
     * published (HTTP 404); throws on transport or server errors so callers can distinguish "gone" from "unreachable".
     */
    suspend fun getNightlyFirmware(): NetworkFirmwareNightly?

    /** Fetches event-firmware display metadata (editions, welcome messages, links) from the Meshtastic API. */
    suspend fun getEventFirmware(): EventFirmwareResponse

    /**
     * Fetches the nRF52 bootloader/OTA quirk catalog (advisory bootloader-upgrade flags and the SoftDevice-variant
     * gating table) from the Meshtastic API. Decodes directly to the same model the bundled asset seed uses — the
     * server serves this file verbatim, so there is nothing to transform.
     */
    suspend fun getBootloaderOtaQuirks(): BootloaderOtaQuirksResponse

    /**
     * Fetches the pinned maintenance-UF2 manifest (factory-erase and OTAFIX bootloader self-update images) from the
     * Meshtastic API. Decodes directly to the same model the bundled asset seed uses — the server serves this file
     * verbatim, so there is nothing to transform. Each image's own `sha256` is still checked against the downloaded
     * bytes before any write; that is unaffected by how this manifest itself is fetched.
     */
    suspend fun getMaintenanceUf2Manifest(): MaintenanceUf2Manifest

    /**
     * Fetches the MQTT root directory (community roots, subdivision neighbors and geocoder segment names) the topic
     * finder places a user with. Decodes directly to the same model the bundled asset seed uses.
     */
    suspend fun getMqttRoots(): MqttRootsResponse
}

/**
 * Ktor-based [ApiService] implementation.
 *
 * Uses relative paths — the base URL is set via the `DefaultRequest` plugin in the platform Koin modules.
 *
 * Registered with `binds = []` to prevent Koin from auto-binding to [ApiService]; host modules (`app`, `desktop`)
 * provide their own explicit `ApiService` binding to allow platform-specific `HttpClient` engines.
 */
@Single(binds = [])
class ApiServiceImpl(private val client: HttpClient) : ApiService {
    override suspend fun getDeviceHardware(): List<NetworkDeviceHardware> = client.get("resource/deviceHardware").body()

    override suspend fun getDeviceLinks(): NetworkDeviceLinksResponse = client.get("resource/deviceLinks").body()

    override suspend fun getFirmwareReleases(): NetworkFirmwareReleases =
        firmwareJson.decodeFromString<List<GitHubRelease>>(client.get(FORK_FIRMWARE_RELEASES_URL).bodyAsText())
            .asNetworkFirmwareReleases()

    override suspend fun getFirmwareReleaseManifest(manifestUrl: String): FirmwareReleaseManifest =
        decodeFirmwareReleaseManifest(client.get(manifestUrl).bodyAsText())

    override suspend fun getNightlyFirmware(): NetworkFirmwareNightly? {
        val response = client.get(NIGHTLY_INDEX_URL)
        return when {
            response.status == HttpStatusCode.NotFound -> null
            response.status.isSuccess() -> firmwareJson.decodeFromString<NetworkFirmwareNightly>(response.bodyAsText())
            else -> error("Unexpected HTTP ${response.status} fetching nightly firmware index")
        }
    }

    override suspend fun getEventFirmware(): EventFirmwareResponse = client.get("resource/eventFirmware").body()

    override suspend fun getBootloaderOtaQuirks(): BootloaderOtaQuirksResponse =
        client.get("resource/bootloaderOtaQuirks").body()

    override suspend fun getMaintenanceUf2Manifest(): MaintenanceUf2Manifest =
        client.get("resource/maintenanceUf2").body()

    override suspend fun getMqttRoots(): MqttRootsResponse = client.get("resource/mqttRoots").body()
}
