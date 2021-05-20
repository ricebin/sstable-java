package org.ricebin.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.ricebin.slice.ByteBufferSlice;
import org.ricebin.slice.Slice;

class SliceUtils {

  static int sharedKeySize(Slice a, Slice b) {
    int minLength = Math.min(a.len(), b.len());
    for (int i = 0; i < minLength; i++) {
      if (a.getByte(i) != b.getByte(i)) {
        return i;
      }
    }
    return minLength;
  }

  static ByteBuffer readFully(FileChannel src, long pos, int len) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
    int bytesRead = src.read(buf, pos);
    if (bytesRead != len) {
      throw new IllegalStateException("unable to read all bytes");
    }
    buf.flip();
    return buf;
  }
}
