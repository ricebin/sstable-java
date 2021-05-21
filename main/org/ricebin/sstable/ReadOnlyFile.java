package org.ricebin.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;

interface ReadOnlyFile {

  ByteBuffer readFully(long pos, int len) throws IOException;

  void close() throws IOException;

  long size() throws IOException;

}
