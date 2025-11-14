package com.github.przemo199.bitcask

import kotlinx.io.Buffer
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

object Utils {
    @OptIn(ExperimentalTime::class)
    fun timestamp(): Int {
        return (Clock.System.now().toEpochMilliseconds() / 1000L).toInt()
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    fun crc32(source: Buffer): Int {
        val tempBuffer = Buffer()
        var crc32 = 0xffffffffU
        source.copyTo(tempBuffer, 0, source.size)

        while (!tempBuffer.exhausted()) {
            val index = (tempBuffer.readByte().toUInt() xor crc32).toUByte()
            crc32 = CRC_32_TABLE[index.toInt()] xor (crc32 shr 8)
        }
        return crc32.toInt()
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    private val CRC_32_TABLE = generateCrc32Table()

    @OptIn(ExperimentalUnsignedTypes::class)
    private fun generateCrc32Table(): UIntArray {
        val table = UIntArray(256)

        for (i in table.indices) {
            table[i] = i.toUInt()
            for (bit in 8 downTo 1) {
                table[i] = if (table[i] % 2U == 0U) {
                    table[i].shr(1)
                } else {
                    table[i].shr(1).xor(0xEDB88320U)
                }
            }
        }

        return table
    }
}
