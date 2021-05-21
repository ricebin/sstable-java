package org.ricebin.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

class FileChannelReadOnlyFile implements ReadOnlyFile {

  private final FileChannel fileChannel;

  FileChannelReadOnlyFile(FileChannel fileChannel) {
    this.fileChannel = fileChannel;
  }

  @Override
  public ByteBuffer readFully(long pos, int len) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
    int bytesRead = fileChannel.read(buf, pos);
    if (bytesRead != len) {
      throw new IllegalStateException("unable to read all bytes");
    }
    buf.flip();
    return buf;
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
  }

  @Override
  public long size() throws IOException {
    return fileChannel.size();
  }
}
