package com.github.przemo199.bitcask

import kotlinx.io.buffered
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readString
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

class MutableBitcaskKtTest {
    private val key = byteArrayOf(1, 2, 3)
    private val value = byteArrayOf(4, 5, 6)
    private val key2 = byteArrayOf(7, 8, 9)
    private val value2 = byteArrayOf(10, 11, 12)
    private val entry = entry(key.decodeToString(), value)
    private val entry2 = entry(key2.decodeToString(), value2)

    @Test
    fun empty() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
            }
        }
    }

    @Test
    fun keyValuePairCanBeSavedAndRetrieved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertEquals(1, bitcask.size)
                assertEquals(1, bitcask.keys.size)
                assertEquals(1, bitcask.values.size)
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
                assertContentEquals(value, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun onlyTheLatestValueIsRetrieved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = byteArrayOf(1, 2, 3)
                bitcask[key.decodeToString()] = value
                assertEquals(1, bitcask.size)
                assertEquals(1, bitcask.keys.size)
                assertEquals(1, bitcask.values.size)
                bitcask.flush()
                assertContentEquals(value, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun keyIsCorrectlyRemoved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                bitcask.flush()
                assertContentEquals(value, bitcask.remove(key.decodeToString()))
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
                assertFalse(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertFalse(bitcask.values.contains(value))
                assertContentEquals(null, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun allKeysAreCorrectlyRemoved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                bitcask.flush()
                assertTrue(bitcask.keys.removeAll(listOf(key.decodeToString())))
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
                assertFalse(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertFalse(bitcask.values.contains(value))
                assertContentEquals(null, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun keyIsCorrectlyRetained() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                bitcask.flush()
                assertFalse(bitcask.keys.retainAll(listOf(key.decodeToString())))
                assertEquals(1, bitcask.size)
                assertEquals(1, bitcask.keys.size)
                assertEquals(1, bitcask.values.size)
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
            }
        }
    }

    @Test
    fun keyValuePairIsCorrectlyRemoved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
                assertTrue(bitcask.entries.contains(entry))
                assertTrue(bitcask.entries.remove(entry))
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
                assertFalse(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertFalse(bitcask.values.contains(value))
                assertFalse(bitcask.entries.contains(entry))
                assertContentEquals(null, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun allKeyValuePairsAreCorrectlyRemoved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
                assertTrue(bitcask.entries.contains(entry))
                assertTrue(bitcask.entries.removeAll(listOf(entry)))
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
                assertFalse(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertFalse(bitcask.values.contains(value))
                assertFalse(bitcask.entries.contains(entry))
                assertContentEquals(null, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun keyValuePairIsCorrectlyRetained() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
                assertTrue(bitcask.entries.contains(entry))
                assertFalse(bitcask.entries.retainAll(listOf(entry)))
                assertEquals(1, bitcask.size)
                assertEquals(1, bitcask.keys.size)
                assertEquals(1, bitcask.values.size)
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
                assertTrue(bitcask.entries.contains(entry))
                assertContentEquals(value, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun valueIsCorrectlyRemoved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertContains(bitcask.values, value)
                assertTrue(bitcask.values.remove(value))
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
                assertFalse(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertFalse(bitcask.values.contains(value))
                assertContentEquals(null, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun allValuesAreCorrectlyRemoved() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertContains(bitcask.values, value)
                assertTrue(bitcask.values.removeAll(listOf(value)))
                assertEquals(0, bitcask.size)
                assertEquals(0, bitcask.keys.size)
                assertEquals(0, bitcask.values.size)
                assertFalse(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertFalse(bitcask.values.contains(value))
                assertContentEquals(null, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun valueIsCorrectlyRetained() {
        TemporaryTestDir().use {
            MutableBitcaskKt(it.path).use { bitcask ->
                bitcask[key.decodeToString()] = value
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertContains(bitcask.values, value)
                assertFalse(bitcask.values.retainAll(listOf(value)))
                assertEquals(1, bitcask.size)
                assertEquals(1, bitcask.keys.size)
                assertEquals(1, bitcask.values.size)
                assertTrue(bitcask.keys.contains(key.decodeToString()))
                bitcask.flush()
                assertTrue(bitcask.values.contains(value))
                assertContentEquals(value, bitcask[key.decodeToString()])
            }
        }
    }

    @Test
    fun onlyOneMutableBitCaskMayBeOpenInOneDirectory() {
        TemporaryTestDir().use { dir ->
            try {
                MutableBitcaskKt(dir.path).use {
                    MutableBitcaskKt(dir.path).use {
                        fail("should be unreachable")
                    }
                }
            } catch (e: Throwable) {
                assertTrue(e is IllegalArgumentException)
            }
        }
    }

    @Test
    fun hintfileIsCorrectlyCreatedAndParsed() {
        TemporaryTestDir().use { dir ->
            MutableBitcaskKt(dir.path).use {
                assertNull(SystemFileSystem.list(dir.path).find { it.name == "hintfile.dat" })
                it[key.decodeToString()] = value
                it[key2.decodeToString()] = value2
            }

            val hintfilePath = SystemFileSystem.list(dir.path).find { it.name == "hintfile.dat" }
            assertNotNull(hintfilePath)
            val hintfileContent = SystemFileSystem.source(hintfilePath).buffered().use { it.readString() }
            assertTrue(hintfileContent.isNotBlank())

            MutableBitcaskKt(dir.path).use {
                assertContentEquals(value, it[key.decodeToString()])
                assertContentEquals(value2, it[key2.decodeToString()])
            }
        }
    }
}
