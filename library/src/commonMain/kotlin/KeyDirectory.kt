package com.github.przemo199.bitcask

/**
 * In-memory key directory mapping keys to their metadata
 */
class KeyDirectory : MutableMap<String, RecordMetadata> by LinkedHashMap() {
    internal fun entriesWithValueSizeOf(size: Int): Sequence<Map.Entry<String, RecordMetadata>> {
       return entries.asSequence()
           .filter { size == it.value.valueSize(it.key) }
    }

    internal fun valueSizeOf(key: String): Int? = this[key]?.valueSize(key)
}
