package org.ricebin.sstable;

import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory.Sink;
import org.ricebin.slice.Slice.Reader;

class BlockTrailer {
  // 1-byte type + 32-bit crc
  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.h#L79
  static final int MAX_ENCODED_LENGTH = 5;

  private final CompressionType compressionType;
  private final int crc32c;

  BlockTrailer(CompressionType compressionType, int crc32c) {
    this.compressionType = compressionType;
    this.crc32c = crc32c;
  }

  static BlockTrailer decode(Slice input) {
    Reader reader = input.newReader();
    CompressionType compressionType = CompressionType.decode(reader.getByte());
    int crc32c = reader.getInt();
    return new BlockTrailer(compressionType, crc32c);
  }

  static Slice encode(BlockTrailer value, Slice.Factory factory) {
    Sink sink = factory.newFixedSizeSink(MAX_ENCODED_LENGTH);
    sink.putByte(value.getCompressionType().getValue());
    sink.putInt(value.getCrc32c());
    return sink.finish();
  }

  CompressionType getCompressionType() {
    return compressionType;
  }

  int getCrc32c() {
    return crc32c;
  }
}
