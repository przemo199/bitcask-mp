package com.github.przemo199.bitcask

import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BitcaskKtTest {
    private val key = byteArrayOf(1, 2, 3)
    private val value = byteArrayOf(4, 5, 6)
    private val key2 = byteArrayOf(7, 8, 9)
    private val value2 = byteArrayOf(10, 11, 12)
    private val entry = entry(key.decodeToString(), value)
    private val entry2 = entry(key2.decodeToString(), value2)
    private val recordBytes = byteArrayOf(-71, 33, 65, 65, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 3) + key + value

    @Test
    fun emptyBitcaskReaderTest() {
        TemporaryTestDir().use {
            val bitcask = BitcaskKt(it.path)
            assertEquals(0, bitcask.size)
            assertEquals(0, bitcask.keys.size)
            assertEquals(0, bitcask.values.size)
            assertEquals(0, bitcask.entries.size)
        }
    }

    @Test
    fun nonEmptyBitcaskReaderTest() {
        TemporaryTestDir().use {
            it.createFile(content = recordBytes)
            val bitcask = BitcaskKt(Path(it.path.name))
            assertEquals(1, bitcask.size)
            assertEquals(1, bitcask.keys.size)
            assertEquals(1, bitcask.values.size)
            assertEquals(1, bitcask.entries.size)
            assertTrue(bitcask.keys.contains(key.decodeToString()))
            assertFalse(bitcask.keys.contains(key2.decodeToString()))
            assertTrue(bitcask.keys.containsAll(listOf(key.decodeToString())))
            assertFalse(bitcask.keys.containsAll(listOf(key2.decodeToString())))
            assertFalse(bitcask.keys.containsAll(listOf(key.decodeToString(), key2.decodeToString())))
            assertTrue(bitcask.values.contains(value))
            assertFalse(bitcask.values.contains(value2))
            assertTrue(bitcask.values.containsAll(listOf(value)))
            assertFalse(bitcask.values.containsAll(listOf(value2)))
            assertFalse(bitcask.values.containsAll(listOf(value, value2)))
            assertTrue(bitcask.entries.contains(entry))
            assertFalse(bitcask.entries.contains(entry2))
            assertTrue(bitcask.entries.containsAll(listOf(entry)))
            assertFalse(bitcask.entries.containsAll(listOf(entry2)))
            assertFalse(bitcask.entries.containsAll(listOf(entry, entry2)))
            assertContentEquals(bitcask[key.decodeToString()], value)
            assertContentEquals(bitcask[key2.decodeToString()], null)
        }
    }

    @Test
    fun bitcaskCorrectlyResolvesAbsoluteDirectory() {
        TemporaryTestDir().use {
            val bitcask = BitcaskKt(it.path)
            assertEquals(SystemFileSystem.resolve(Path(it.path)), bitcask.absoluteDirectory)
        }
    }
}
