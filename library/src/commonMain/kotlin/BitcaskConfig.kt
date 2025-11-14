package com.github.przemo199.bitcask

import kotlinx.io.files.FileSystem
import kotlinx.io.files.SystemFileSystem

data class BitcaskConfig(
    val syncOnPut: Boolean = false,
    val maxFileSize: Int = 1024 * 1024 * 10, // 10 MB
    val dataFileSuffix: String = "dat",
    val tombstoneValue: ByteArray = "BITCASK_TOMBSTONE".encodeToByteArray(),
    val expiryTimeout: Int? = null,
    val fileSystem: FileSystem = SystemFileSystem
)
