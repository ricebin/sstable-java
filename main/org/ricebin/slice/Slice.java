package org.ricebin.slice;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;

// https://github.com/google/leveldb/blob/master/include/leveldb/slice.h
public interface Slice {

  int SIZE_OF_LONG = 8;

  int SIZE_OF_INT = 4;

  int MAX_VAR_INT_32_SIZE = 5;

  int MAX_VAR_INT_64_SIZE = 9;

  int len();

  byte getByte(int index);

  int getUnsignedByte(int pos);

  int getInt(int pos);

  long getLong(int pos);

  Slice slice(int pos, int length);

  void put(ByteBuffer sink);

  Reader newReader();


  interface Reader {

    byte getByte();

    int getUnsignedByte();

    int getInt();

    int getVarInt();

    long getLong();

    long getVarLong();

    void position(int pos);

    int position();

    int len();

    boolean hasRemaining();

    Slice slice(int len);

    void getBytes(byte[] sink, int sinkOffset, int len);

    void write(FileChannel dst) throws IOException;
  }

  interface Factory<T extends Slice> {

    T readFully(FileChannel src, long pos, int len) throws IOException;

    Sink<T> newFixedSizeSink(int size);

    ReusableSink<T> newDynamicSink(int initialSize);

    T wrap(byte[] buf, int offset, int len);

    ByteBufferSlice empty();

    Comparator<T> comparator();

    interface Sink<T extends Slice> {

      int position();

      Sink<T> putByte(byte value);

      Sink<T> putInt(int value);

      Sink<T> putVarInt(int value);

      Sink<T> putLong(long value);

      Sink<T> putVarLong(long value);

      Sink<T> putSlice(Slice slice);

      T finish();

      int currentSize();
    }

    interface ReusableSink<T extends Slice> extends Sink<T> {
      void reset();
    }
  }
}
