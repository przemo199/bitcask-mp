package com.github.przemo199.bitcask

import kotlinx.io.Sink
import kotlinx.io.IOException
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlin.time.Clock
import kotlin.time.ExperimentalTime

/**
 * Represents a file to which [BitcaskRecord]s can be written
 *
 * @param directory directory in which the file will be created
 * @param config Bitcask configuration
 */
@OptIn(ExperimentalTime::class)
data class WriteFile
@Throws(IOException::class)
constructor(val directory: Path, val config: BitcaskConfig) : AutoCloseable {
    private var writePosition: Int = 0
    private val sink: Sink
    val path: Path
    val size: Int
        get() = writePosition

    init {
        require(config.fileSystem.metadataOrNull(directory)?.isDirectory == true) {
            "Directory $directory does not exist"
        }

        path = Path("${Clock.System.now().toEpochMilliseconds()}.${config.dataFileSuffix}")
        val filePath = Path(directory, path.name)
        require(!config.fileSystem.exists(filePath)) { "File $path already exists" }
        sink = config.fileSystem.sink(filePath).buffered()
    }

    /**
     * Appends [BitcaskRecord] to the file
     *
     * @return metadata of the written record
     */
    fun append(record: BitcaskRecord): RecordMetadata {
        val writeStartPosition = writePosition
        record.writeTo(sink)
        if (config.syncOnPut) flush()
        writePosition += record.size
        return RecordMetadata(path, writeStartPosition, record)
    }

    /**
     * Writes all buffered data to disk
     */
    fun flush() = sink.flush()

    override fun close() {
        flush()
        sink.close()
    }
}
