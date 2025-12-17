package com.github.przemo199.bitcask

/**
 * Read-only Bitcask interface
 */
interface Bitcask : Map<String, ByteArray> {
    /**
     * Accumulates value starting with [initial] value and applying [operation]
     * to current accumulator value and each element.
     *
     * Returns the specified [initial] value if the sequence is empty.
     *
     * @param [operation] function that takes current accumulator value and an element, and calculates the next accumulator value.
     */
    fun <T> fold(initial: T, operation: (String, ByteArray, T) -> T): T
}

/**
 * Mutable Bitcask interface
 */
interface MutableBitcask : Bitcask, MutableMap<String, ByteArray>, AutoCloseable {
    /**
     * Associates the specified [value] with the specified [key]
     */
    operator fun set(key: String, value: ByteArray)

    /**
     * Removes the value associated with this [key]
     *
     * @param [key] key to remove value by
     */
    fun delete(key: String)

    /**
     * Compacts the data files on disk by writing the current state of Bitcask and deleting old files
     */
    fun merge()

    /**
     * Writes all buffered data to disk
     */
    fun flush()
}
