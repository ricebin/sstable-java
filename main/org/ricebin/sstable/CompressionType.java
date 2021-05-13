package org.ricebin.sstable;

// https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/include/leveldb/options.h#L25

public enum CompressionType {
  NONE((byte) 0);
  // TODO(ricebin): snappy(1)

  private final byte value;

  CompressionType(byte value) {
    this.value = value;
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
}
