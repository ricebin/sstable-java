package org.ricebin.sstable;

import com.google.common.primitives.Ints;
import java.util.Objects;
import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory.Sink;
import org.ricebin.slice.Slice.Reader;

// https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.h#L23
class BlockHandle {

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.h#L26
  static final int MAX_ENCODED_LENGTH = 10 + 10;

  private final long offset;
  private final int size;

  BlockHandle(long offset, int size) {
    if (offset < 0 || size <= 0) {
      throw new IllegalArgumentException();
    }
    this.offset = offset;
    this.size = size;
  }

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.cc#L23
  static BlockHandle decode(Reader input) {
    long offset = input.getVarLong();
    int size = Ints.checkedCast(input.getVarLong());
    return new BlockHandle(offset, size);
  }

  static Slice encode(BlockHandle value, Slice.Factory factory) {
    Sink sink = factory.newFixedSizeSink(MAX_ENCODED_LENGTH);
    encode(value, sink);
    return sink.finish();
  }

  static void encode(BlockHandle value, Sink sink) {
    sink
        .putVarLong(value.offset)
        .putVarLong(value.size);
  }

  long getOffset() {
    return offset;
  }

  int getSize() {
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockHandle that = (BlockHandle) o;
    return offset == that.offset && size == that.size;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, size);
  }
}
