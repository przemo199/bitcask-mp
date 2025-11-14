package com.github.przemo199.bitcask

import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.files.Path
import kotlinx.io.readString
import kotlin.io.encoding.Base64.Default.encodeToByteArray

/**
 * Metadata of a record stored in a data file
 *
 * @property fileName The name of the data file where the record is stored
 * @property recordSize The size of the record in bytes
 * @property recordPosition The position of the record in the data file
 * @property timestamp The timestamp when the record was created or last updated
 */
data class RecordMetadata(
    val fileName: Path,
    val recordSize: Int,
    val recordPosition: Int,
    val timestamp: Int = Utils.timestamp()
) {
    constructor(fileName: Path, recordPosition: Int, record: BitcaskRecord) : this(
        fileName,
        record.size,
        recordPosition,
        record.timestamp
    )

    fun writeTo(sink: Sink) {
        sink.apply {
            fileName.name.encodeToByteArray().let {
                writeInt(it.size)
                write(it)
            }
            writeInt(recordSize)
            writeInt(recordPosition)
            writeInt(timestamp)
        }
    }

    fun isExpired(timeout: Int?): Boolean {
        timeout ?: return false
        return Utils.timestamp() > timestamp + timeout
    }

    fun valueSize(key: String) = recordSize - key.encodeToByteArray().size - BitcaskRecord.METADATA_SIZE

    companion object {
        fun fromByteArray(bytes: ByteArray): RecordMetadata = Buffer().let {
            it.write(bytes)
            fromSource(it)
        }

        fun fromSource(source: Source): RecordMetadata = source.let {
            val fileNameLength = it.readInt()
            val fileName = it.readString(fileNameLength.toLong())
            val valueSize = it.readInt()
            val recordPosition = it.readInt()
            val timestamp = it.readInt()
            return RecordMetadata(Path(fileName), valueSize, recordPosition, timestamp)
        }
    }
}
