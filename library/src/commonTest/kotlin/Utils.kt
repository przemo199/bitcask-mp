package com.github.przemo199.bitcask

fun entry(key: String, value: ByteArray): MutableMap.MutableEntry<String, ByteArray> = object : MutableMap.MutableEntry<String, ByteArray> {
    override val key: String = key
    override val value: ByteArray = value
    override fun setValue(newValue: ByteArray): ByteArray = throw UnsupportedOperationException()
}
