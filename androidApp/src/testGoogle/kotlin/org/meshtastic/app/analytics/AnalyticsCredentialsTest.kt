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
package org.meshtastic.app.analytics

import java.io.File
import java.util.Properties
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AnalyticsCredentialsTest {

    @Test
    fun `the tracked default Datadog token is a placeholder`() {
        val defaults = Properties().apply { trackedFile("secrets.defaults.properties").reader().use(::load) }

        assertTrue(isPlaceholderDatadogToken(defaults.getProperty("datadogClientToken")))
    }

    @Test
    fun `a missing Datadog token is a placeholder, a real-looking one is not`() {
        assertTrue(isPlaceholderDatadogToken(""))
        assertFalse(isPlaceholderDatadogToken("pub0123456789abcdef0123456789abcdef"))
    }

    @Test
    fun `the tracked google-services json values are a placeholder project`() {
        // CI replaces androidApp/google-services.json with the real file, so its tracked values are checked here.
        assertTrue(isPlaceholderFirebaseProject("xxx", "APlaceholderAPIKeyWith-ThirtyNineCharsX"))
    }

    @Test
    fun `a real project is not a placeholder, a missing one is`() {
        assertFalse(isPlaceholderFirebaseProject("meshtastic-app", "AIzaSyExampleExampleExampleExample0000"))
        assertTrue(isPlaceholderFirebaseProject(null, null))
        assertTrue(isPlaceholderFirebaseProject("meshtastic-app", null))
    }

    /** Finds [path] from the repository root; Gradle runs unit tests from the module directory. */
    private fun trackedFile(path: String): File =
        generateSequence(File("").absoluteFile) { it.parentFile }.map { File(it, path) }.first { it.isFile }
}
