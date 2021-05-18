package org.ricebin.sstable;

import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Reader;

// https://github.com/google/leveldb/blob/master/util/hash.cc
class Hash {

  static int hash(Slice slice, int seed) {
    int len = slice.len();
    // Similar to murmur hash
    int m = 0xc6a4a793;
    int r = 24;

    int h = seed ^ (len * m);

    Reader reader = slice.newReader();

    int idx = 0;
    // Pick up four bytes at a time
    for (; idx + 4 <= len; idx += 4) {
      int w = reader.getInt();
      h += w;
      h *= m;
      h ^= (h >>> 16);
    }

    // Pick up remaining bytes
    final int remaining = len - idx;
    switch (remaining) {
      case 3:
        h += slice.getUnsignedByte(idx + 2) << 16;
        // FALLTHROUGH INTENDED;
      case 2:
        h += slice.getUnsignedByte(idx + 1) << 8;
        // FALLTHROUGH INTENDED;
      case 1:
        h += slice.getUnsignedByte(idx);
        h *= m;
        h ^= (h >>> r);
        break;
    }
    return h;
  }

}
