package org.ricebin.sstable;

import java.util.Collection;
import org.ricebin.slice.Slice;

public interface FilterPolicy {

  interface Writer {

    String name();

    byte[] createFilter(Collection<Slice> keys);
  }

  interface Reader {

    String name();

    boolean keyMayMatch(Slice key, Slice filter);
  }

}
