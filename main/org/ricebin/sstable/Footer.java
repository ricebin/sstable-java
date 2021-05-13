package org.ricebin.sstable;

import java.util.Objects;
import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory.Sink;
import org.ricebin.slice.Slice.Reader;

class Footer {

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.h#L53
  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  static int MAX_ENCODED_LENGTH = BlockHandle.MAX_ENCODED_LENGTH * 2 + Slice.SIZE_OF_LONG;


  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.h#L76
  // kTableMagicNumber was picked by running
  // echo http://code.google.com/p/leveldb/ | sha1sum
  // and taking the leading 64 bits.
  static long MAGIC_NUMBER = 0xdb4775248b80fb57L;

  private final BlockHandle metaIndex;
  private final BlockHandle index;

  Footer(BlockHandle metaIndex, BlockHandle index) {
    this.metaIndex = metaIndex;
    this.index = index;
  }

  BlockHandle getMetaIndex() {
    return metaIndex;
  }

  BlockHandle getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Footer footer = (Footer) o;
    return Objects.equals(metaIndex, footer.metaIndex) && Objects
        .equals(index, footer.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metaIndex, index);
  }

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.cc#L42
  static Footer decode(Slice slice) {
    Reader input = slice.newReader();
    BlockHandle metaIndex = BlockHandle.decode(input);
    BlockHandle index = BlockHandle.decode(input);

    // magic number is always last 8
    // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.cc#L43

    // TODO(ricebin): check magic number
    long magicNumber = readMagicNumber(slice, MAX_ENCODED_LENGTH - Slice.SIZE_OF_LONG);
    if (magicNumber != MAGIC_NUMBER) {
      throw new IllegalStateException("not an sstable (bad magic number)");
    }

    return new Footer(metaIndex, index);
  }

  static Slice encode(Footer footer, Slice.Factory factory) {
    Sink sink = factory.newFixedSizeSink(MAX_ENCODED_LENGTH);
    encode(footer, sink);
    return sink.finish();
  }

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/format.cc#L31
  static void encode(Footer footer, Sink sink) {
    // check has capacity >= MAX_ENCODED_LENGTH
    int start = sink.position();

    BlockHandle.encode(footer.metaIndex, sink);
    BlockHandle.encode(footer.index, sink);

    // TODO(ricebin): prob something more efficient than this
    // write padding
    while (sink.position() < start + MAX_ENCODED_LENGTH - Slice.SIZE_OF_LONG) {
      sink.putByte((byte) 0);
    }

    encodeMagicNumber(sink);
  }

  static void encodeMagicNumber(Sink sink) {
    sink.putLong(MAGIC_NUMBER);
  }

  static long readMagicNumber(Slice slice, int pos) {
    return slice.getLong(pos);
  }
}
