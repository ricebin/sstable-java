package org.ricebin.sstable;

// https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/include/leveldb/options.h#L25

import java.nio.ByteBuffer;

public enum CompressionType {
  NONE((byte) 0, new Compressor() {
    @Override
    public ByteBuffer uncompress(ByteBuffer input) {
      return input;
    }

    @Override
    public ByteBuffer compress(ByteBuffer buf) {
      return buf;
    }
  });

  private final byte value;
  private final Compressor impl;

  CompressionType(byte value, Compressor impl) {
    this.value = value;
    this.impl = impl;
  }

  public static CompressionType decode(byte value) {
    if (value == 0) {
      return NONE;
    } else {
      throw new IllegalArgumentException("unsupported compression: " + value);
    }
  }

  public byte getValue() {
    return value;
  }

  Compressor getCompressor() {
    return impl;
  }
}
