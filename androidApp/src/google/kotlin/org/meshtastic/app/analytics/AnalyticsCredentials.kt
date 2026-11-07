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

/**
 * Whether [clientToken] is the tracked placeholder from `secrets.defaults.properties` (or missing), rather than a token
 * the build was given. Datadog's intake refuses every upload made with a placeholder, so the SDK would only spend
 * battery, storage and network on data that can never arrive.
 */
internal fun isPlaceholderDatadogToken(clientToken: String): Boolean =
    clientToken.isBlank() || clientToken.startsWith(PLACEHOLDER_DATADOG_TOKEN_PREFIX)

/**
 * Whether the Firebase project the build was configured with is the tracked placeholder `google-services.json` (project
 * `xxx`, API key `APlaceholder…`), or missing. Crashlytics and Analytics cannot deliver anything to it.
 */
internal fun isPlaceholderFirebaseProject(projectId: String?, apiKey: String?): Boolean = projectId.isNullOrBlank() ||
    projectId == PLACEHOLDER_FIREBASE_PROJECT_ID ||
    apiKey.isNullOrBlank() ||
    apiKey.startsWith(PLACEHOLDER_FIREBASE_API_KEY_PREFIX)

private const val PLACEHOLDER_DATADOG_TOKEN_PREFIX = "fake"
private const val PLACEHOLDER_FIREBASE_PROJECT_ID = "xxx"
private const val PLACEHOLDER_FIREBASE_API_KEY_PREFIX = "APlaceholder"
