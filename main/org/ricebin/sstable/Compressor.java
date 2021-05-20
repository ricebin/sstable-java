package org.ricebin.sstable;

import java.nio.ByteBuffer;

interface Compressor {

  ByteBuffer uncompress(ByteBuffer input);

  ByteBuffer compress(ByteBuffer buf);
}
