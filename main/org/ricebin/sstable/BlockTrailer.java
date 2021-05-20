package org.ricebin.sstable;

import java.nio.ByteBuffer;
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

  static BlockTrailer decode(ByteBuffer input) {
    CompressionType compressionType = CompressionType.decode(input.get());
    int crc32c = input.getInt();
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
