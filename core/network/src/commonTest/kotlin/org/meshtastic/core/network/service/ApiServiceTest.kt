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
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.respondError
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.SerializationException
import org.meshtastic.core.model.FirmwareTarget
import org.meshtastic.core.network.di.CoreNetworkModule
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class ApiServiceTest {
    @Test
    fun `nightly pointer is fetched from the nightly host root`() = runTest {
        var requested: String? = null
        val engine = MockEngine { request ->
            requested = request.url.toString()
            respond(
                content = """{"version":"2.8.1.0becda3","id":"v2.8.1.0becda3","commit":"0becda3"}""",
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }
        val client = HttpClient(engine)

        try {
            val nightly = ApiServiceImpl(client).getNightlyFirmware()

            assertEquals("https://nightly.meshtastic.org/index.json", requested)
            assertEquals("2.8.1.0becda3", nightly?.version)
        } finally {
            client.close()
        }
    }

    @Test
    fun `firmware releases come from the fork github releases`() = runTest {
        var requested: String? = null
        val dl = "https://github.com/prokrypt/firmware/releases/download"
        val engine = MockEngine { request ->
            requested = request.url.toString()
            respond(
                content =
                """
                    [
                      {"tag_name": "v2.8.3.aaaaaaa", "draft": true, "assets": []},
                      {"tag_name": "v2.8.2.0ld0000", "draft": false, "published_at": "2026-11-07T18:27:42Z",
                       "assets": [{"name": "firmware-2.8.2.0ld0000.json",
                                   "browser_download_url": "$dl/v2.8.2.0ld0000/firmware-2.8.2.0ld0000.json"}]},
                      {"tag_name": "v2.8.2.926f09e", "name": "Firmware (no-auto-writes) 2.8.2.926f09e Alpha",
                       "published_at": "2026-11-08T09:06:37Z",
                       "html_url": "https://github.com/prokrypt/firmware/releases/tag/v2.8.2.926f09e",
                       "body": null, "draft": false, "prerelease": true,
                       "assets": [
                         {"name": "firmware-nrf52840-2.8.2.926f09e.zip",
                          "browser_download_url": "$dl/v2.8.2.926f09e/firmware-nrf52840-2.8.2.926f09e.zip"},
                         {"name": "firmware-2.8.2.926f09e.json",
                          "browser_download_url": "$dl/v2.8.2.926f09e/firmware-2.8.2.926f09e.json"}
                       ]},
                      {"tag_name": "v2.8.1.0000000", "draft": false, "assets": []},
                      {"tag_name": "v2.8.2-pk.0003", "draft": false, "published_at": "2026-11-08T08:00:00Z",
                       "assets": [{"name": "firmware-2.8.2.0ld0001.json",
                                   "browser_download_url": "$dl/v2.8.2-pk.0003/firmware-2.8.2.0ld0001.json"}]}
                    ]
                    """
                    .trimIndent(),
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }
        val client = HttpClient(engine)

        try {
            val releases = ApiServiceImpl(client).getFirmwareReleases().releases

            assertEquals("https://api.github.com/repos/prokrypt/firmware/releases?per_page=20", requested)
            // Only the newest published release with a manifest: drafts, manifest-less and older ones are dropped.
            val release = releases.alpha.single()
            assertEquals(releases.alpha, releases.stable)
            assertEquals("v2.8.2.926f09e", release.id)
            assertEquals("Firmware (no-auto-writes) 2.8.2.926f09e Alpha", release.title)
            assertEquals("$dl/v2.8.2.926f09e/firmware-2.8.2.926f09e.json", release.zipUrl)
            assertEquals("", release.releaseNotes)
        } finally {
            client.close()
        }
    }

    @Test
    fun `fork release id is the firmware version from the manifest name, not the tag`() {
        val dl = "https://github.com/prokrypt/firmware/releases/download/v2.8.2-pk.0015"
        val release =
            listOf(
                GitHubRelease(
                    tagName = "v2.8.2-pk.0015",
                    publishedAt = "2026-11-08T09:06:37Z",
                    assets =
                    listOf(
                        GitHubAsset("firmware-nrf52840-2.8.2.21a9f20.zip", "$dl/firmware-nrf52840-2.8.2.21a9f20.zip"),
                        GitHubAsset("firmware-2.8.2.21a9f20.json", "$dl/firmware-2.8.2.21a9f20.json"),
                    ),
                ),
            )
                .asNetworkFirmwareReleases()
                .releases
                .alpha
                .single()

        assertEquals("v2.8.2.21a9f20", release.id)
        assertEquals("v2.8.2-pk.0015", release.title)
        assertEquals("$dl/firmware-2.8.2.21a9f20.json", release.zipUrl)
    }

    @Test
    fun `nightly pointer returns null when nothing is published`() = runTest {
        // The host answers an unpublished nightly with its own HTML error page, so the status — not the body —
        // is what distinguishes "gone" from "unreachable".
        val engine = MockEngine { respondError(HttpStatusCode.NotFound, "<!doctype html><title>Not Found</title>") }
        val client = HttpClient(engine)

        try {
            assertNull(ApiServiceImpl(client).getNightlyFirmware())
        } finally {
            client.close()
        }
    }

    @Test
    fun `mqtt roots are fetched from the resource path and tolerate unknown fields`() = runTest {
        var requestedPath: String? = null
        val engine = MockEngine { request ->
            requestedPath = request.url.encodedPath
            respond(
                content =
                """
                    {
                      "entries": [{"root": "msh/US/MeshSTL", "name": "MeshSTL", "center": [38.581, -90.374],
                                   "radiusKm": 50, "future": true}],
                      "neighbors": {"US-TX": ["US-OK"], "US-OK": ["US-TX"]},
                      "subdivisionSegments": {"US": {"washington": "WA"}},
                      "generatedAt": "ignored"
                    }
                    """
                    .trimIndent(),
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }
        val client = HttpClient(engine) { install(ContentNegotiation) { json(CoreNetworkModule().provideJson()) } }

        try {
            val roots = ApiServiceImpl(client).getMqttRoots()

            assertEquals("/resource/mqttRoots", requestedPath)
            assertEquals(listOf(38.581, -90.374), roots.entries.single().center)
            assertEquals(50.0, roots.entries.single().radiusKm)
            assertEquals(listOf("US-OK"), roots.neighbors["US-TX"])
            assertEquals("WA", roots.subdivisionSegments["US"]?.get("washington"))
        } finally {
            client.close()
        }
    }

    @Test
    fun `malformed mqtt roots fail the fetch`() = runTest {
        val engine = MockEngine {
            respond(
                content = """{"entries": [{"name": "no root"}]}""",
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }
        val client = HttpClient(engine) { install(ContentNegotiation) { json(CoreNetworkModule().provideJson()) } }

        try {
            assertFailsWith<Exception> { ApiServiceImpl(client).getMqttRoots() }
        } finally {
            client.close()
        }
    }

    @Test
    fun `service decodes release manifest served as octet stream`() = runTest {
        val manifestUrl = "https://downloads.example/firmware/manifest.json"
        val engine = MockEngine { request ->
            assertEquals(manifestUrl, request.url.toString())
            respond(
                content =
                """
                    {
                      "version": "2.7.26.54e0d8d",
                      "targets": [
                        {"board": "t-deck", "platform": "esp32"}
                      ]
                    }
                    """
                    .trimIndent(),
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.OctetStream.toString()),
            )
        }
        val client = HttpClient(engine) { install(ContentNegotiation) { json() } }

        try {
            val manifest = ApiServiceImpl(client).getFirmwareReleaseManifest(manifestUrl)

            assertEquals("2.7.26.54e0d8d", manifest.version)
            assertEquals(listOf(FirmwareTarget(board = "t-deck", platform = "esp32")), manifest.targets)
        } finally {
            client.close()
        }
    }

    @Test
    fun `firmware release manifest decoder accepts release asset JSON and unknown fields`() {
        val manifest =
            decodeFirmwareReleaseManifest(
                """
                {
                  "version": "2.7.26.54e0d8d",
                  "targets": [
                    {"board": "t-deck", "platform": "esp32", "future": true}
                  ],
                  "unknown": "ignored"
                }
                """
                    .trimIndent(),
            )

        assertEquals("2.7.26.54e0d8d", manifest.version)
        assertEquals(listOf(FirmwareTarget(board = "t-deck", platform = "esp32")), manifest.targets)
    }

    @Test
    fun `firmware release manifest decoder coerces explicit nulls to defaults`() {
        val manifest =
            decodeFirmwareReleaseManifest(
                """
                {
                  "version": null,
                  "targets": [
                    {
                      "board": "t-deck",
                      "platform": null
                    }
                  ]
                }
                """
                    .trimIndent(),
            )

        assertEquals("", manifest.version)
        assertEquals(FirmwareTarget(board = "t-deck", platform = ""), manifest.targets.single())
    }

    @Test
    fun `firmware release manifest decoder rejects malformed JSON`() {
        assertFailsWith<SerializationException> { decodeFirmwareReleaseManifest("not-json") }
    }
}
