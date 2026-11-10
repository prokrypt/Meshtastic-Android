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
package org.meshtastic.core.database

import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.longPreferencesKey
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DatabaseManagerRetirementTest : DatabaseManagerTestFixture() {

    @BeforeTest fun setUp() = setUpFixture()

    @AfterTest fun tearDown() = tearDownFixture()

    @Test
    fun sourcePoolStaysOpenAfterLogicalRetirement() = runTest(testDispatcher) {
        val (dbA, dbB) = setupTwoDatabases()
        assertEquals(dbB, manager.currentDb.value, "addrB's DB should be active before association")
        val sourceDb = manager.currentDb.value

        // Successful merge logically retires the source but does not close its pool.
        manager.associateCurrentDevice(nodeNum = 123, deviceId = "deadbeefdeadbeef")
        assertEquals(dbA, manager.currentDb.value, "destination must be canonical after successful merge")

        // Direct DAO read on the retained source reference — must succeed (pool still open).
        val sourceMyNode = sourceDb.nodeInfoDao().getMyNodeInfo().first()
        assertNull(sourceMyNode, "source read must succeed (pool open); source content is empty by default")

        val (writers, waiters) = manager.debugWriterCounts()
        assertEquals(0, writers, "no leaked writers after association")
        assertEquals(0, waiters, "no pending drain waiters after association")
        assertTrue(buildDbName("addrB") in armableDs.data.first()[retiredDbNamesKey].orEmpty())
        assertTrue(manager.deletedDatabaseNames.isEmpty(), "same-process retirement must not delete the source")
    }

    @Test
    fun sessionRolloverDuringMergeRollsBackAssociationAndKeepsSourceActive() = runTest(testDispatcher) {
        val (canonical, source) = setupTwoDatabases()
        var sessionActive = true
        manager.beforeMerge = { _, _, _ -> sessionActive = false }

        manager.associateCurrentDevice(
            nodeNum = 123,
            deviceId = "deadbeefdeadbeef",
            isSessionActive = { sessionActive },
        )

        val sourceName = buildDbName("addrB")
        assertEquals(source, manager.currentDb.value, "stale association must leave the source active")
        assertFalse(canonical.mergeMarkerDao().isMerged(sourceName), "stale merge must roll back its marker")
        assertTrue(sourceName !in armableDs.data.first()[retiredDbNamesKey].orEmpty())
        assertTrue(manager.deletedDatabaseNames.isEmpty())
    }

    @Test
    fun persistedRetirementIsReclaimedOnceBeforeFirstDeviceDatabaseOpen() = runTest(testDispatcher) {
        val retiredName = buildDbName("retired-address")
        val lastUsedKey = longPreferencesKey("db_last_used:$retiredName")
        armableDs.edit {
            it[retiredDbNamesKey] = setOf(retiredName)
            it[lastUsedKey] = 123L
        }
        val restartedManager = createManager()

        restartedManager.switchActiveDatabase("addrA")

        assertEquals(listOf(retiredName), restartedManager.deletedDatabaseNames)
        val cleanedPrefs = armableDs.data.first()
        assertTrue(retiredName !in cleanedPrefs[retiredDbNamesKey].orEmpty())
        assertNull(cleanedPrefs[lastUsedKey])

        restartedManager.switchActiveDatabase("addrB")
        assertEquals(
            listOf(retiredName),
            restartedManager.deletedDatabaseNames,
            "cleanup must run once per lifetime",
        )
    }

    @Test
    fun cachedPoolRemainsTrackedWhenCloseFails() = runTest(testDispatcher) {
        manager.switchActiveDatabase("addrA")
        val addrADatabase = manager.currentDb.value
        manager.switchActiveDatabase("addrB")
        val addrAName = buildDbName("addrA")
        manager.failCloseFor = addrADatabase

        assertFailsWith<IllegalStateException> { manager.closeCachedForTest(addrAName) }

        manager.failCloseFor = null
        manager.closeCachedForTest(addrAName)
        assertEquals(2, manager.closeAttempts.count { it === addrADatabase })
        assertEquals(1, manager.closedDatabases.count { it === addrADatabase })
    }

    @Test
    fun shutdownDoesNotDeleteRetiredFileWhenItsPoolFailsToClose() = runTest(testDispatcher) {
        val (_, retiredSource) = setupTwoDatabases()
        manager.associateCurrentDevice(nodeNum = 123, deviceId = "deadbeefdeadbeef")
        val retiredName = buildDbName("addrB")
        manager.failCloseFor = retiredSource

        manager.close()

        assertTrue(retiredName !in manager.deletedDatabaseNames)
        assertTrue(retiredName in armableDs.data.first()[retiredDbNamesKey].orEmpty())
        assertFalse(manager.debugAcceptingWrites())

        manager.failCloseFor = null
        manager.close()

        assertEquals(2, manager.closeAttempts.count { it === retiredSource })
        assertEquals(1, manager.closedDatabases.count { it === retiredSource })
        assertEquals(1, manager.deletedDatabaseNames.count { it == retiredName })
        assertTrue(retiredName !in armableDs.data.first()[retiredDbNamesKey].orEmpty())
    }

    @Test
    fun radioOnAnotherRadiosUsbAddressGetsItsOwnDatabase() = runTest(testDispatcher) {
        val usb = "s/dev/bus/usb/001/002"
        val nameA = buildDbName(usb)
        manager.switchActiveDatabase(usb)
        manager.associateCurrentDevice(nodeNum = 123, deviceId = "deadbeefdeadbeef")
        val dbA = manager.currentDb.value

        // After a phone reboot the same bus path is another radio, which opens radio A's DB by address.
        manager.associateCurrentDevice(nodeNum = 456, deviceId = "feedfacefeedface")
        val dbB = manager.currentDb.value
        assertTrue(dbB !== dbA, "the second radio must not stay on the first radio's DB")
        var prefs = armableDs.data.first()
        val nameB = assertNotNull(prefs[deviceDbPrefKey("feedfacefeedface")])
        assertEquals(nameA, prefs[deviceDbPrefKey("deadbeefdeadbeef")], "radio A keeps its claim")
        assertFalse(dbB.mergeMarkerDao().isMerged(nameA), "radio A's DB must not be merged into radio B's")

        // Radio A back on that path opens B's DB by address, then returns to its own.
        manager.switchActiveDatabase(null)
        manager.switchActiveDatabase(usb)
        assertEquals(dbB, manager.currentDb.value)
        manager.associateCurrentDevice(nodeNum = 123, deviceId = "deadbeefdeadbeef")
        assertEquals(dbA, manager.currentDb.value)
        prefs = armableDs.data.first()
        assertEquals(nameB, prefs[deviceDbPrefKey("feedfacefeedface")], "radio B keeps its claim")
        assertFalse(dbA.mergeMarkerDao().isMerged(nameB), "radio B's DB must not be merged into radio A's")
        assertTrue(prefs[retiredDbNamesKey].orEmpty().isEmpty(), "neither radio's DB is retired")
        assertTrue(manager.deletedDatabaseNames.isEmpty())
    }
}
