package com.github.przemo199.bitcask

/**
 * Exception thrown when a record is found to be corrupted
 */
class RecordCorruptedException: RuntimeException("record corrupted")
