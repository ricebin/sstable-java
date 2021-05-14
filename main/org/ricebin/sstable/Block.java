package org.ricebin.sstable;

import java.util.Iterator;
import java.util.Map.Entry;
import org.ricebin.slice.Slice;

interface Block<V> {

  Iterator<Entry<Slice, V>> iterator();

  Iterator<Entry<Slice, V>> iterator(Slice lowerBound);
}
