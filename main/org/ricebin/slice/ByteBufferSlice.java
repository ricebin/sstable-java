package org.ricebin.slice;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ricebin.slice.Slice.Factory.ReusableSink;
import org.ricebin.slice.Slice.Factory.Sink;

public class ByteBufferSlice implements Slice, Comparable<ByteBufferSlice> {

  private static final int HIGH_BIT_MASK = 128;

  private final ByteBuffer buf;
  private final int offset;
  private final int len;

  ByteBufferSlice(ByteBuffer buf, int offset, int len) {
    this.buf = buf;
    this.offset = offset;
    this.len = len;
  }

  @Override
  public int compareTo(ByteBufferSlice other) {
    int minLen = Math.min(len, other.len);
    byte[] a = buf.array();
    byte[] b = other.buf.array();
    for (int i=0; i<minLen; i++) {
      int v1 = a[offset + i] & 0xFF; // zero extended
      int v2 = b[other.offset + i] & 0xFF; // zero extended
      if (v1 != v2) {
        return v1 - v2;
      }
    }
    return len - other.len;
  }

  @Override
  public byte getByte(int pos) {
    if (pos < 0) {
      throw new IllegalArgumentException();
    }
    return buf.get(offset + pos);
  }

  @Override
  public int getUnsignedByte(int pos) {
    if (pos < 0) {
      throw new IllegalArgumentException();
    }
    return buf.get(offset + pos) & 0xff;
  }

  @Override
  public int len() {
    return len;
  }

  @Override
  public int getInt(int index) {
    if (index < 0) {
      throw new IllegalArgumentException();
    }
    return buf.getInt(index + offset);
  }

  @Override
  public long getLong(int index) {
    if (index < 0) {
      throw new IllegalArgumentException();
    }
    return buf.getLong(index + offset);
  }


  @Override
  public Slice slice(int index, int length) {
    if (index >= 0 && index + length <= len) {
      return new ByteBufferSlice(buf, index + offset, length);
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public void put(ByteBuffer sink) {
    sink.put(shallowCopy());
  }


  @Override
  public Reader newReader() {
    return new ReaderImpl(shallowCopy(), offset);
  }

  private ByteBuffer shallowCopy() {
    return duplicate(buf)
        .position(offset)
        .limit(offset + len);
  }

  private static class ReaderImpl implements Reader {

    private final ByteBuffer buf;
    private final int offset;

    private ReaderImpl(ByteBuffer buf, int offset) {
      this.buf = buf;
      this.offset = offset;
    }

    @Override
    public byte getByte() {
      return buf.get();
    }

    @Override
    public int getUnsignedByte() {
      return buf.get() & 0xff;
    }

    @Override
    public int getInt() {
      return buf.getInt();
    }

    // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/util/coding.cc#L86
    @Override
    public int getVarInt() {
      int result = 0;
      for (int shift = 0; shift <= 28; shift += 7) {
        int b = buf.get();
        if ((b & HIGH_BIT_MASK) == 0) {
          result |= (b << shift);
          return result;
        } else {
          // more bytes are present
          result |= ((b & 127) << shift);
        }
      }
      throw new IllegalStateException();
    }

    // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/util/coding.cc#L116
    @Override
    public long getVarLong() {
      long result = 0;
      for (int shift = 0; shift <= 63; shift += 7) {
        long b = buf.get() & 0xFF;
        if ((b & HIGH_BIT_MASK) == 0) {
          result |= (b << shift);
          return result;
        } else {
          // more bytes are present
          result |= ((b & 127) << shift);
        }
      }
      throw new IllegalStateException();
    }

    @Override
    public void position(int pos) {
      buf.position(offset + pos);
    }

    @Override
    public int position() {
      return buf.position() - offset;
    }

    @Override
    public int len() {
      return buf.limit() - offset;
    }

    @Override
    public boolean hasRemaining() {
      return buf.hasRemaining();
    }

    @Override
    public Slice slice(int len) {
      ByteBuffer copy = duplicate(buf);
      return new ByteBufferSlice(copy, copy.position(), len);
    }

    @Override
    public void getBytes(byte[] sink, int sinkOffset, int len) {
      buf.get(sink, sinkOffset, len);
    }

    @Override
    public void write(FileChannel dst) throws IOException {
      dst.write(buf);
      if (buf.hasRemaining()) {
        throw new IllegalStateException("still have unwritten bytes");
      }
    }

    @Override
    public long getLong() {
      return buf.getLong();
    }
  }


  static ByteBufferSlice create(byte[] buf) {
    return new ByteBufferSlice(ByteBuffer.wrap(buf), 0, buf.length);
  }

  static ByteBufferSlice create(byte[] buf, int offset, int length) {
    return new ByteBufferSlice(ByteBuffer.wrap(buf), offset, length);
  }

  public static ByteBufferSlice wrap(ByteBuffer buf) {
    return new ByteBufferSlice(buf, 0, buf.limit());
  }


  private static final ByteBufferSlice EMPTY = new ByteBufferSlice(allocate(0), 0, 0);

  public static final Factory<ByteBufferSlice> FACTORY = new Factory<ByteBufferSlice>() {

    @Override
    public ByteBufferSlice readFully(FileChannel src, long pos, int len) throws IOException {
      ByteBuffer buf = allocate(len);
      int bytesRead = src.read(buf, pos);
      if (bytesRead != len) {
        throw new IllegalStateException("unable to read all bytes");
      }
      buf.flip();
      return ByteBufferSlice.wrap(buf);
    }

    @Override
    public Sink<ByteBufferSlice> newFixedSizeSink(int size) {
      return new SinkImpl(size);
    }

    @Override
    public ReusableSink<ByteBufferSlice> newDynamicSink(int initialSize) {
      return new DynamicReusableSinkImpl(initialSize);
    }

    @Override
    public ByteBufferSlice wrap(byte[] buf, int offset, int len) {
      return ByteBufferSlice.create(buf, offset, len);
    }

    @Override
    public ByteBufferSlice empty() {
      return EMPTY;
    }

    @Override
    public Comparator<ByteBufferSlice> comparator() {
      return Comparator.naturalOrder();
    }
  };

  private static class SinkImpl implements Sink<ByteBufferSlice> {

    ByteBuffer sink;
    final AtomicBoolean finished = new AtomicBoolean(false);

    SinkImpl(int size) {
      this.sink = allocate(size);
    }

    @Override
    public final int position() {
      return sink.position();
    }

    @Override
    public Sink<ByteBufferSlice> putByte(byte value) {
      sink.put(value);
      return this;
    }

    @Override
    public Sink<ByteBufferSlice> putInt(int value) {
      sink.putInt(value);
      return this;
    }

    // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/util/coding.cc#L21
    @Override
    public Sink<ByteBufferSlice> putVarInt(int v) {
      if (v < 0) {
        throw new UnsupportedOperationException();
      } else if (v < (1 << 7)) {
        sink.put((byte) v);
      } else if (v < (1 << 14)) {
        sink.put((byte) (v | HIGH_BIT_MASK));
        sink.put((byte) (v >> 7));
      } else if (v < (1 << 21)) {
        sink.put((byte) (v | HIGH_BIT_MASK));
        sink.put((byte) ((v >> 7) | HIGH_BIT_MASK));
        sink.put((byte) (v >> 14));
      } else if (v < (1 << 28)) {
        sink.put((byte) (v | HIGH_BIT_MASK));
        sink.put((byte) ((v >> 7) | HIGH_BIT_MASK));
        sink.put((byte) ((v >> 14) | HIGH_BIT_MASK));
        sink.put((byte) (v >> 21));
      } else {
        sink.put((byte) (v | HIGH_BIT_MASK));
        sink.put((byte) ((v >> 7) | HIGH_BIT_MASK));
        sink.put((byte) ((v >> 14) | HIGH_BIT_MASK));
        sink.put((byte) ((v >> 21) | HIGH_BIT_MASK));
        sink.put((byte) (v >> 28));
      }
      return this;
    }

    // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/util/coding.cc#L55
    @Override
    public Sink<ByteBufferSlice> putVarLong(long v) {
      if (v < 0) {
        throw new UnsupportedOperationException();
      }
      while (v >= HIGH_BIT_MASK) {
        sink.put((byte) (v | HIGH_BIT_MASK));
        v >>= 7;
      }
      sink.put((byte) v);
      return this;
    }

    @Override
    public Sink<ByteBufferSlice> putLong(long value) {
      sink.putLong(value);
      return this;
    }

    @Override
    public Sink<ByteBufferSlice> putSlice(Slice slice) {
      slice.put(sink);
      return this;
    }

    @Override
    public final ByteBufferSlice finish() {
      if (!finished.compareAndSet(false, true)) {
        throw new IllegalStateException("already finished");
      }
      sink.flip();
      return ByteBufferSlice.wrap(sink);

    }

    @Override
    public final int currentSize() {
      return sink.position();
    }
  }

  private static class DynamicReusableSinkImpl extends SinkImpl implements ReusableSink<ByteBufferSlice> {

    DynamicReusableSinkImpl(int initialSize) {
      super(initialSize);
    }

    private void ensureSpace(int remainingRequired) {
      if (sink.remaining() < remainingRequired) {
        // TODO(ricebin): configurable growth factor
        int totalRequired = sink.position() + remainingRequired + 100;
        ByteBuffer newSink = allocate(totalRequired);
        sink.flip();
        newSink.put(sink);
        sink = newSink;
      }
    }

    @Override
    public Sink<ByteBufferSlice> putByte(byte value) {
      ensureSpace(1);
      return super.putByte(value);
    }

    @Override
    public Sink<ByteBufferSlice> putInt(int value) {
      ensureSpace(SIZE_OF_INT);
      return super.putInt(value);
    }

    @Override
    public Sink<ByteBufferSlice> putVarInt(int value) {
      ensureSpace(MAX_VAR_INT_32_SIZE);
      return super.putVarInt(value);
    }

    @Override
    public Sink<ByteBufferSlice> putLong(long value) {
      ensureSpace(SIZE_OF_LONG);
      return super.putLong(value);
    }

    @Override
    public Sink<ByteBufferSlice> putVarLong(long value) {
      ensureSpace(MAX_VAR_INT_64_SIZE);
      return super.putVarLong(value);
    }

    @Override
    public Sink<ByteBufferSlice> putSlice(Slice slice) {
      ensureSpace(slice.len());
      return super.putSlice(slice);
    }

    @Override
    public final void reset() {
      if (!finished.compareAndSet(true, false)) {
        throw new IllegalStateException("already finished");
      }
      sink.clear();
    }
  }

  static ByteBuffer allocate(int size) {
    return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
  }

  static ByteBuffer duplicate(ByteBuffer buf) {
    return buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
  }

}