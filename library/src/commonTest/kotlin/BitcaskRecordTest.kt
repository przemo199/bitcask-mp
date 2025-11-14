package com.github.przemo199.bitcask

import kotlinx.io.readByteArray
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class BitcaskRecordTest {
    private val key = byteArrayOf(1, 2, 3)
    private val value = byteArrayOf(4, 5, 6)
    private val bitcaskRecord = BitcaskRecord(key, value, timestamp = 1)
    private val recordBytes = byteArrayOf(-71, 33, 65, 65, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 3) + key + value

    @Test
    fun emptySizeIsCalculatedCorrectly() {
        val record = BitcaskRecord(byteArrayOf(), byteArrayOf())
        assertEquals(BitcaskRecord.METADATA_SIZE, record.size)
    }

    @Test
    fun nonemptySizeIsCalculatedCorrectly() {
        assertEquals(BitcaskRecord.METADATA_SIZE + key.size + value.size, bitcaskRecord.size)
    }

    @Test
    fun recordIsCorrectlyConvertedToByteArray() {
        assertContentEquals(recordBytes, bitcaskRecord.toByteArray())
    }

    @Test
    fun recordIsCorrectlyConvertedToSource() {
        val source = bitcaskRecord.toSource()
        assertContentEquals(recordBytes, source.readByteArray())
    }

    @Test
    fun recordIsCorrectlyConvertedFromByteArray() {
        val record = BitcaskRecord.fromByteArray(recordBytes)
        assertEquals(bitcaskRecord, record)
    }

    @Test
    fun exceptionIsThrownWhenConsistencyCheckFails() {
        val corruptedRecordBytes = recordBytes.copyOf().also { it[it.lastIndex] = 0 }
        try {
            BitcaskRecord.fromByteArray(corruptedRecordBytes)
            fail("should be unreachable")
        } catch (e: Throwable) {
            assertTrue(e is RuntimeException)
        }
    }
}
