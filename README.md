# bitcask-mp

Bitcask-mp is a [Bitcask](https://riak.com/assets/bitcask-intro.pdf) key/value store implementation using Kotlin Multiplatform, available on JVM, JS, Wasm and native targets.  
This implementation is possible thanks to [kotlinx-io](https://github.com/Kotlin/kotlinx-io).

## Usage
Create read/write instance of Bitcask
```kotlin
MutableBitcaskKt(Path("/bitcask-dir")).use {
    mutableBitcask.set("key", "value".encodeToByteArray())
    mutableBitcask.flush()
    val value = mutableBitcask.get("key")
    assertEquals("value", value.decodeToString())
}
```
or read only instance
```kotlin
// assuming that code in the previous block was executed
val bitcask = BitcaskKt(Path("/bitcask-dir"))
val value = bticask.get("key")
assertEquals("value", value.decodeToString())
```

> **_NOTE:_** This library attempts to make bitcask implementation compatible with ```Map<String, ByteArray>``` interface,
> but some operations may be slow, especially those involving datasets with values of the same length.
