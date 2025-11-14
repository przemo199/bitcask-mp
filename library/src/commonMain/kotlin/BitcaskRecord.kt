package com.github.przemo199.bitcask

import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readByteArray

/**
 * A data class representing a record in the Bitcask key/value store.
 *
 * Each record consists of a key, a value, and a timestamp. The class provides methods to
 * serialize and deserialize records to and from byte arrays and sources, as well as to
 * write records to sinks. It also includes a CRC32 checksum for data integrity verification.
 *
 * @property key The key of the record as a byte array.
 * @property value The value of the record as a byte array.
 * @property timestamp The timestamp of the record, defaulting to the current time.
 * @property size The total size of the record including metadata.
 * @property crc32 The CRC32 checksum of the record, calculated lazily.
 */
data class BitcaskRecord(
    val key: ByteArray,
    val value: ByteArray,
    val timestamp: Int = Utils.timestamp()
) : Comparable<BitcaskRecord> {
    val size = key.size + value.size + METADATA_SIZE
    val crc32: Int by lazy { Utils.crc32(bufferWithoutCrc32) }
    private val bufferWithoutCrc32 by lazy {
        Buffer().apply {
            writeInt(timestamp)
            writeInt(key.size)
            writeInt(value.size)
            write(key)
            write(value)
        }
    }

    constructor(key: String, value: ByteArray, timestamp: Int = Utils.timestamp()) : this(
        key.encodeToByteArray(),
        value,
        timestamp
    )

    fun toSource(): Source {
        return Buffer().apply {
            writeInt(crc32)
            bufferWithoutCrc32.copyTo(this)
        }
    }

    fun toByteArray(): ByteArray = Buffer().also(::writeTo).readByteArray()

    fun writeTo(sink: Sink) {
        sink.apply {
            writeInt(crc32)
            write(bufferWithoutCrc32, bufferWithoutCrc32.size)
        }
    }

    override fun compareTo(other: BitcaskRecord): Int = timestamp.compareTo(other.timestamp)

    override fun equals(other: Any?): Boolean {
        return this === other ||
            other is BitcaskRecord &&
            timestamp == other.timestamp &&
            key contentEquals other.key &&
            value contentEquals other.value
    }

    override fun hashCode(): Int = arrayOf(key, value, timestamp).contentDeepHashCode()

    companion object {
        const val METADATA_SIZE = Int.SIZE_BYTES * 4

        fun fromByteArray(bytes: ByteArray): BitcaskRecord = Buffer().let {
            it.write(bytes)
            fromSource(it)
        }

        fun fromSource(source: Source): BitcaskRecord = source.let {
            val crc32 = it.readInt()
            val timestamp = it.readInt()
            val keySize = it.readInt()
            val valueSize = it.readInt()
            val key = it.readByteArray(keySize)
            val value = it.readByteArray(valueSize)
            BitcaskRecord(key, value, timestamp).also {
                if (crc32 != it.crc32) {
                    throw RecordCorruptedException()
                }
            }
        }
    }
}
