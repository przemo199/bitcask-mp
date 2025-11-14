package com.github.przemo199.bitcask

import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TemporaryTestDirTest {
    @Test
    fun temporaryTestDirIsCreatedAndDestroyed() {
        TemporaryTestDir().apply {
            use {
                assertTrue(exists())
            }
            assertFalse(exists())
        }
    }

    @Test
    fun temporaryFileCanBeCreatedInTemporaryTestDir() {
        val fileName = "temporaryFile"
        val filePath: Path
        val bytes = byteArrayOf(1, 2, 3)
        TemporaryTestDir().use { dir ->
            filePath = dir.createFile(fileName, bytes)
            assertTrue(SystemFileSystem.exists(filePath))
            SystemFileSystem.source(filePath).buffered().use {
                assertContentEquals(bytes, it.readByteArray())
            }
        }
        assertFalse(SystemFileSystem.exists(filePath))
    }
}
