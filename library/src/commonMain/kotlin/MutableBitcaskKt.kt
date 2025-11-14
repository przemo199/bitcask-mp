package com.github.przemo199.bitcask

import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlin.sequences.fold

/**
 * Kotlin Multiplatform implementation of mutable Bitcask key/value store
 *
 * @param directory directory where data files will be stored
 * @param config configuration of the Bitcask instance
 */
open class MutableBitcaskKt(directory: Path, config: BitcaskConfig = BitcaskConfig()) :
    BitcaskKt(directory, config), MutableBitcask {

    init {
        LockedDirs.verifyAndAdd(absoluteDirectory)
    }

    private var writeFile: WriteFile = WriteFile(directory, config)

    /**
     * Checks if the size of current [WriteFile] has exceeded configured [BitcaskConfig.maxFileSize]
     */
    protected fun shouldCreateNewWriteFile(): Boolean = writeFile.size >= config.maxFileSize

    fun createNewWriteFile() {
        writeFile.flush()
        writeFile.close()
        writeFile = WriteFile(absoluteDirectory, config)
    }

    override fun set(key: String, value: ByteArray) {
        keyDirectory[key] = saveRecord(BitcaskRecord(key, value))
    }

    protected fun saveRecord(record: BitcaskRecord): RecordMetadata {
        if (shouldCreateNewWriteFile()) {
            createNewWriteFile()
        }
        return writeFile.append(record)
    }

    override fun put(key: String, value: ByteArray): ByteArray? = this[key].also { set(key, value) }

    override fun delete(key: String) {
        keyDirectory[key]?.let { saveTombstone(key) }
    }

    /**
     * Creates and saves tombstone
     */
    protected fun saveTombstone(key: String) {
        val tombstoneRecord = createTombstone(key)
        saveRecord(tombstoneRecord)
        keyDirectory -= key
    }

    /**
     * Creates [BitcaskRecord] representing tombstone
     */
    protected fun createTombstone(key: String) = BitcaskRecord(key, config.tombstoneValue)

    override fun merge() {
        val oldDataFiles = getSortedDataFiles()
        createNewWriteFile()
        keys.forEach { saveRecord(getRecord(it)!!) }
        oldDataFiles.forEach(config.fileSystem::delete)
        createHintfile()
    }

    override fun flush() = writeFile.flush()

    override val keys: MutableSet<String> = object : MutableSet<String>, Set<String> by super@MutableBitcaskKt.keys {
        override fun add(element: String): Boolean = throw UnsupportedOperationException()

        override fun addAll(elements: Collection<String>): Boolean = throw UnsupportedOperationException()

        override fun clear() = ArrayList(keyDirectory.keys).forEach(::saveTombstone)

        override fun iterator(): MutableIterator<String> = object : MutableIterator<String> {
            private val keyIterator = keyDirectory.keys.iterator()
            private var key: String? = null

            override fun hasNext(): Boolean = keyIterator.hasNext()

            override fun next(): String = keyIterator.next().also(::key::set)

            override fun remove() {
                saveTombstone(key ?: throw IllegalStateException())
                key = null
            }
        }

        override fun remove(element: String): Boolean {
            if (element in this) {
                saveTombstone(element)
                return true
            }
            return false
        }

        override fun removeAll(elements: Collection<String>): Boolean =
            elements.asSequence().map(::remove).fold(false, Boolean::or)

        override fun retainAll(elements: Collection<String>): Boolean =
            elements.asSequence().filterNot(::contains).map(::remove).fold(false, Boolean::or)
    }

    override val values: MutableCollection<ByteArray> = object :
        MutableCollection<ByteArray>, Collection<ByteArray> by super@MutableBitcaskKt.values {

        override fun add(element: ByteArray): Boolean = throw UnsupportedOperationException()

        override fun addAll(elements: Collection<ByteArray>): Boolean = throw UnsupportedOperationException()

        override fun clear() = keyDirectory.clear()

        override fun iterator() = object : MutableIterator<ByteArray> {
            private val keyIterator = keyDirectory.keys.iterator()
            private var key: String? = null

            override fun hasNext(): Boolean = keyIterator.hasNext()

            override fun next(): ByteArray {
                key = keyIterator.next()
                return get(key)!!
            }

            override fun remove() {
                saveTombstone(key ?: throw IllegalStateException())
                key = null
            }
        }

        override fun remove(element: ByteArray): Boolean {
            keyDirectory.entriesWithValueSizeOf(element.size).find { element contentEquals get(it.key) }
                ?.let {
                    saveTombstone(it.key)
                    return true
                }
            return false
        }

        override fun removeAll(elements: Collection<ByteArray>): Boolean =
            elements.asSequence().map(::remove).fold(false, Boolean::or)

        override fun retainAll(elements: Collection<ByteArray>): Boolean {
            var anyRemoved = false
            val elementsBySize = elements.groupBy(ByteArray::size)
            keyDirectory.entries.forEach { entry ->
                elementsBySize[entry.value.valueSize(entry.key)]?.let {
                    if (it.any(get(entry.key)::contentEquals)) {
                        return@forEach
                    }
                }
                saveTombstone(entry.key)
                anyRemoved = true
            }
            return anyRemoved
        }
    }

    override val entries: MutableSet<MutableMap.MutableEntry<String, ByteArray>> = object :
        MutableSet<MutableMap.MutableEntry<String, ByteArray>>,
        Set<MutableMap.MutableEntry<String, ByteArray>> by super@MutableBitcaskKt.entries as Set<MutableMap.MutableEntry<String, ByteArray>> {

        override fun add(element: MutableMap.MutableEntry<String, ByteArray>): Boolean {
            if (element in this) {
                return false
            }
            set(element.key, element.value)
            return true
        }

        override fun addAll(elements: Collection<MutableMap.MutableEntry<String, ByteArray>>): Boolean {
            return elements.asSequence().map(::add).fold(false, Boolean::or)
        }

        override fun clear() = keys.forEach(::saveTombstone)


        override fun iterator() = object : MutableIterator<MutableMap.MutableEntry<String, ByteArray>> {
            private val keyIterator = keyDirectory.keys.iterator()
            private var currentValue: MutableMap.MutableEntry<String, ByteArray>? = null

            override fun hasNext(): Boolean = keyIterator.hasNext()

            override fun next(): MutableMap.MutableEntry<String, ByteArray> {
                return MutableBitcaskEntry(keyIterator.next()).also(::currentValue::set)
            }

            override fun remove() {
                saveTombstone((currentValue ?: throw IllegalStateException()).key)
                currentValue = null
            }
        }

        override fun remove(element: MutableMap.MutableEntry<String, ByteArray>): Boolean {
            val hasPossibleHit = keyDirectory[element.key]?.let {
                it.valueSize(element.key) == element.value.size
            } ?: false
            if (hasPossibleHit && element.value contentEquals get(element.key)) {
                saveTombstone(element.key)
                return true
            }
            return false
        }

        override fun removeAll(elements: Collection<MutableMap.MutableEntry<String, ByteArray>>): Boolean {
            return elements.asSequence().map(::remove).fold(false, Boolean::or)
        }

        override fun retainAll(elements: Collection<MutableMap.MutableEntry<String, ByteArray>>): Boolean {
            var anyRemoved = false
            val keyToElementToRetainMap = elements.associateBy { it.key }
            keyDirectory.entries.forEach { entry ->
                keyToElementToRetainMap[entry.key]?.let {
                    if (it.value.size == entry.value.valueSize(it.key) && it.value contentEquals get(entry.key)) {
                        return@forEach
                    }
                }
                saveTombstone(entry.key)
                anyRemoved = true
            }
            return anyRemoved
        }
    }

    inner class MutableBitcaskEntry(key: String) : BitcaskEntry(key), MutableMap.MutableEntry<String, ByteArray> {
        override fun setValue(newValue: ByteArray): ByteArray = put(key, newValue)!!
    }

    override fun clear() = keys.forEach(::saveTombstone)

    override fun putAll(from: Map<out String, ByteArray>) {
        from.forEach { (key, value) -> put(key, value) }
    }

    override fun remove(key: String): ByteArray? = keyDirectory[key]
        ?.let(::loadRecordSource)
        ?.use(BitcaskRecord::fromSource)
        ?.also { saveTombstone(key) }
        ?.value

    protected fun createHintfile() {
        val hintfilePath = Path(absoluteDirectory, "hintfile.dat")
        val oldHintfile = Path(absoluteDirectory, "hintfile-old.dat")
        if (config.fileSystem.exists(hintfilePath)) {
            config.fileSystem.source(hintfilePath).buffered().use { source ->
                config.fileSystem.sink(oldHintfile).buffered().use {
                    it.transferFrom(source)
                }
            }
        }
        config.fileSystem.sink(hintfilePath).buffered().use {
            keyDirectory.entries.forEach { (key, value) ->
                key.encodeToByteArray().let { keyBytes ->
                    it.writeInt(keyBytes.size)
                    it.write(keyBytes)
                }
                value.writeTo(it)
            }
        }
        config.fileSystem.delete(oldHintfile, false)
    }

    override fun close() {
        writeFile.flush()
        writeFile.close()
        createHintfile()
        LockedDirs.remove(absoluteDirectory)
    }

    companion object {
        private val LOCKED_DIRS: MutableSet<Path> = mutableSetOf()

        private object LockedDirs {
            private val lock = ReentrantLock()

            fun add(path: Path) {
                verifyPath(path)
                lock.withLock {
                    LOCKED_DIRS += path
                }
            }

            fun remove(path: Path) {
                verifyPath(path)
                lock.withLock {
                    LOCKED_DIRS -= path
                }
            }

            fun contains(path: Path): Boolean {
                verifyPath(path)
                return lock.withLock { path in LOCKED_DIRS }
            }

            /**
             * Verifies that [MutableBitcaskKt] instance is not already created in this directory
             *
             * @param path absolute path to the directory
             */
            fun verifyAndAdd(path: Path) {
                verifyPath(path)
                lock.withLock {
                    require(path !in LOCKED_DIRS) { "MutableBitcask instance already exists in this directory" }
                    LOCKED_DIRS += path
                }
            }

            private fun verifyPath(path: Path) {
                require(path.isAbsolute) { "Path must be absolute" }
            }
        }
    }
}
