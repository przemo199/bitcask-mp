package com.github.przemo199.bitcask

import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class TemporaryTestDir @OptIn(ExperimentalUuidApi::class) constructor(directoryName: String = Uuid.random().toString()) : AutoCloseable {
    var path: Path = Path(directoryName)

    init {
        SystemFileSystem.createDirectories(path)
    }

    override fun close() {
        SystemFileSystem.list(path).forEach(SystemFileSystem::delete)
        SystemFileSystem.delete(path)
    }

    fun exists() = SystemFileSystem.exists(path)

    @OptIn(ExperimentalUuidApi::class)
    fun createFile(name: String = Uuid.random().toString(), content: ByteArray): Path = Path(path, "$name.dat").apply {
        SystemFileSystem.sink(this).buffered().use {
            it.write(content)
            it.flush()
        }
    }
}
