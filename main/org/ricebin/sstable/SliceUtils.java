package org.ricebin.sstable;

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
}
