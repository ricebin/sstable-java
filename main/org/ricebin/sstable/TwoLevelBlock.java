package org.ricebin.sstable;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.function.Function;
import org.ricebin.slice.Slice;

class TwoLevelBlock<V> {

  final PrefixBlock<V> blockIndex;
  private final Function<V, PrefixBlock<Slice>> getBlock;

  TwoLevelBlock(PrefixBlock<V> blockIndex, Function<V, PrefixBlock<Slice>> getBlock) {
    this.blockIndex = blockIndex;
    this.getBlock = getBlock;
  }

  Iterator<PrefixBlock<Slice>> iterator() {
    return Iterators.transform(
        blockIndex.iterator(),
        e -> getBlock.apply(e.getValue()));
  }

  Iterator<PrefixBlock<Slice>> iterator(Slice key) {
    return Iterators.transform(
        blockIndex.iterator(key),
        e -> getBlock.apply(e.getValue()));
  }
}
