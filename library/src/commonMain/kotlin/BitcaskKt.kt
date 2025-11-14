package com.github.przemo199.bitcask

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.io.Source
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.readString
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock

private val logger = KotlinLogging.logger {}

/**
 * Kotlin Multiplatform implementation of Bitcask key/value store
 *
 * @param directory directory where data files will be read from
 * @param config configuration of the Bitcask instance
 */
open class BitcaskKt(
    directory: Path,
    val config: BitcaskConfig = BitcaskConfig()
) : Bitcask {
    protected val keyDirectory: KeyDirectory
    val absoluteDirectory: Path

    init {
        config.fileSystem.createDirectories(directory)
        absoluteDirectory = config.fileSystem.resolve(directory)
        keyDirectory = KeyDirectories.getOrPut(absoluteDirectory, ::loadKeyDirectory)
    }

    /**
     * Retrieves [BitcaskRecord] associated with the [key]
     */
    protected fun getRecord(key: String): BitcaskRecord? = keyDirectory[key]
        ?.takeUnless { it.isExpired(config.expiryTimeout) }
        ?.let(::loadRecordSource)
        ?.use(BitcaskRecord::fromSource)

    override val size: Int
        get() = keyDirectory.size

    override fun isEmpty(): Boolean = keyDirectory.isEmpty()

    override fun containsKey(key: String): Boolean = key in keyDirectory

    override fun containsValue(value: ByteArray): Boolean = value in values

    override fun get(key: String): ByteArray? = getRecord(key)?.value

    override val keys: Set<String> = keyDirectory.keys

    override val values: Collection<ByteArray> = object : Collection<ByteArray> {
        override val size: Int
            get() = keyDirectory.size

        override fun contains(element: ByteArray): Boolean {
            return keyDirectory.entriesWithValueSizeOf(element.size).any { element contentEquals get(it.key) }
        }

        override fun containsAll(elements: Collection<ByteArray>): Boolean {
            data class ByteArrayWrapper(val array: ByteArray) {
                override fun equals(other: Any?): Boolean {
                    return this === other || other is ByteArrayWrapper && array.contentEquals(other.array)
                }

                override fun hashCode(): Int = array.contentHashCode()
            }

            val distinctElements = elements
                .map(::ByteArrayWrapper)
                .distinct()
                .map(ByteArrayWrapper::array)

            val elementsBySize = distinctElements.groupBy(ByteArray::size)
            val elementsToFindCount = elementsBySize.values.sumOf(Collection<ByteArray>::size)
            val entriesToBeCheckedCount =
                elementsBySize.keys.map(keyDirectory::entriesWithValueSizeOf).sumOf { it.count() }
            if (elementsToFindCount > entriesToBeCheckedCount) {
                return false
            }
            var foundElementsCount = 0
            return elementsBySize.all { (size, element) ->
                keyDirectory.entriesWithValueSizeOf(size).forEach { entry ->
                    element.forEach {
                        if (it contentEquals get(entry.key)) {
                            foundElementsCount++
                            if (foundElementsCount == distinctElements.size) return@all true
                        }
                    }
                }
                return@all false
            }
        }

        override fun isEmpty(): Boolean = keyDirectory.isEmpty()

        override fun iterator(): Iterator<ByteArray> = object : Iterator<ByteArray> {
            private val keyIterator = keyDirectory.keys.iterator()

            override fun hasNext(): Boolean = keyIterator.hasNext()

            override fun next(): ByteArray = get(keyIterator.next())!!
        }
    }

    override val entries: Set<Map.Entry<String, ByteArray>> = object : Set<Map.Entry<String, ByteArray>> {
        override val size: Int
            get() = keyDirectory.size

        override fun isEmpty(): Boolean = keyDirectory.isEmpty()

        override fun contains(element: Map.Entry<String, ByteArray>): Boolean {
            return element.value.size == keyDirectory.valueSizeOf(element.key) &&
                    element.value contentEquals get(element.key)
        }

        override fun iterator() = object : Iterator<Map.Entry<String, ByteArray>> {
            private val keyIterator = keyDirectory.keys.iterator()

            override fun hasNext(): Boolean = keyIterator.hasNext()

            override fun next(): Map.Entry<String, ByteArray> = BitcaskEntry(keyIterator.next())
        }

        override fun containsAll(elements: Collection<Map.Entry<String, ByteArray>>): Boolean {
            val allEntriesHavePossibleMatches = elements.all {
                it.value.size == keyDirectory.valueSizeOf(it.key)
            }
            return allEntriesHavePossibleMatches && elements.all(::contains)
        }
    }

    /**
     * Represents a key/value pair held by a [BitcaskKt]
     */
    open inner class BitcaskEntry(override val key: String) : Map.Entry<String, ByteArray> {
        override val value: ByteArray
            get() = get(key)!!
    }

    final override inline fun <T> fold(initial: T, operation: (String, ByteArray, T) -> T): T {
        var accumulator = initial
        keys.forEach { accumulator = operation(it, get(it)!!, accumulator) }
        return accumulator
    }

    protected fun loadRecordSource(metadata: RecordMetadata): Source {
        val filePath = Path(absoluteDirectory, metadata.fileName.name)
        return config.fileSystem.source(filePath).buffered().also {
            it.skip(metadata.recordPosition.toLong())
            it.require(metadata.recordSize.toLong())
        }
    }

    protected fun getSortedDataFiles(): List<Path> = config.fileSystem.list(absoluteDirectory)
        .filter {
            it.name.endsWith(config.dataFileSuffix) && config.fileSystem.metadataOrNull(it)?.isRegularFile == true
        }
        .sortedBy(Path::name)

    private fun loadKeyDirectory(): KeyDirectory {
        val hintfilePath = Path(absoluteDirectory, "hintfile.dat")
        if (config.fileSystem.metadataOrNull(hintfilePath)?.isRegularFile == true) {
            logger.info { "Found hintfile, rebuilding key directory" }
            try {
                return rebuildKeyDirectoryFromHintfile(hintfilePath)
            } catch (e: Exception) {
                logger.error(e) { "Encountered problem when loading key directory from hintfile" }
            }
        }

        val dataFiles = getSortedDataFiles()
        if (dataFiles.isNotEmpty()) {
            logger.info { "Found datafile(s), rebuilding key directory" }
            try {
                return rebuildKeyDirectoryFromDataFiles(dataFiles)
            } catch (e: Exception) {
                logger.info { "Failed to rebuild key directory from previous state" }
                throw e
            }
        }
        return KeyDirectory()
    }

    private fun rebuildKeyDirectoryFromHintfile(hintfilePath: Path): KeyDirectory = KeyDirectory().apply {
        config.fileSystem.source(hintfilePath).buffered().use {
            while (!it.exhausted()) {
                val keySize = it.readInt()
                val key = it.readString(keySize.toLong())
                this[key] = RecordMetadata.fromSource(it)
            }
        }
    }

    private fun rebuildKeyDirectoryFromDataFiles(dataFiles: List<Path>): KeyDirectory = KeyDirectory().apply {
        dataFiles.forEach { file ->
            var recordPosition = 0
            config.fileSystem.source(file).buffered().use {
                while (!it.exhausted()) {
                    val record = BitcaskRecord.fromSource(it)
                    if (record.value contentEquals config.tombstoneValue) {
                        this -= record.key.decodeToString()
                    } else {
                        this[record.key.decodeToString()] = RecordMetadata(file, recordPosition, record)
                    }
                    recordPosition += record.size
                }
            }
        }
    }

    companion object {
        private val KEY_DIRECTORIES: MutableMap<Path, KeyDirectory> = mutableMapOf()

        private object KeyDirectories {
            private val lock = ReentrantLock()

            fun getOrPut(key: Path, defaultValue: () -> KeyDirectory): KeyDirectory {
                return lock.withLock {
                    KEY_DIRECTORIES.getOrPut(key, defaultValue)
                }
            }
        }
    }
}
