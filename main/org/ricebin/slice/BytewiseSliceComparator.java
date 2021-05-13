package org.ricebin.slice;

import java.util.Comparator;

public class BytewiseSliceComparator implements Comparator<Slice> {

  public static final BytewiseSliceComparator INSTANCE = new BytewiseSliceComparator();

  @Override
  public int compare(Slice a, Slice b) {
    int minLen = Math.min(a.len(), b.len());
    for (int i=0; i<minLen; i++) {
      int v1 = a.getUnsignedByte(i); // zero extended
      int v2 = b.getUnsignedByte(i); // zero extended
      if (v1 != v2) {
        return v1 - v2;
      }
    }
    return a.len() - b.len();
  }

}
