package org.ricebin.sstable;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.function.Function;
import org.ricebin.slice.Slice;

class PrefixBlockIndex {

  final PrefixBlock<BlockHandle> blockIndex;
  private final Function<BlockHandle, PrefixBlock<Slice>> getBlock;

  PrefixBlockIndex(PrefixBlock<BlockHandle> blockIndex, Function<BlockHandle, PrefixBlock<Slice>> getBlock) {
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
