package org.ricebin.sstable;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Function;
import org.ricebin.slice.Slice;

class TwoLevelBlock<F, T> implements Block<T> {

  final Block<F> blockIndex;
  private final Function<F, Block<T>> getBlock;

  TwoLevelBlock(Block<F> blockIndex, Function<F, Block<T>> getBlock) {
    this.blockIndex = blockIndex;
    this.getBlock = getBlock;
  }

  @Override
  public Iterator<Entry<Slice, T>> iterator() {
    return Iterators.concat(
        Iterators.transform(
            blockIndex.iterator(),
            e -> getBlock.apply(e.getValue()).iterator()));
  }

  @Override
  public Iterator<Entry<Slice, T>> iterator(Slice lowerBound) {
    return Iterators.concat(
        Iterators.transform(
            blockIndex.iterator(lowerBound),
            e -> getBlock.apply(e.getValue()).iterator(lowerBound)));
  }
}
