package org.ricebin.sstable;

import org.ricebin.slice.Slice;

public interface FilterPolicy {

  // TODO(ricebin): writer

  interface Reader {

    String name();

    boolean keyMayMatch(Slice key, Slice filter);
  }

  Reader getReader();
}
