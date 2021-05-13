package org.ricebin.slice;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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

  interface Factory {

    Slice readFully(FileChannel src, long pos, int len) throws IOException;

    Sink newFixedSizeSink(int size);

    ReusableSink newDynamicSink(int initialSize);

    Slice wrap(byte[] buf, int offset, int len);

    ByteBufferSlice empty();

    interface Sink {

      int position();

      Sink putByte(byte value);

      Sink putInt(int value);

      Sink putVarInt(int value);

      Sink putLong(long value);

      Sink putVarLong(long value);

      Sink putSlice(Slice slice);

      Slice finish();

      int currentSize();
    }

    interface ReusableSink extends Sink {
      void reset();
    }
  }
}
