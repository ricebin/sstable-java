package org.ricebin.sstable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory;

// https://github.com/google/leveldb/blob/master/table/block_builder.cc
class PrefixBlockBuilder implements BlockBuilder {

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/include/leveldb/options.h#L105
  static final int RESTART_INTERVAL = 16;

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/include/leveldb/options.h#L100
  // TODO(ricebin): configure via options
  static final int BLOCK_SIZE = 4 * 1024;

  private final Slice.Factory sliceFactory;
  private final Comparator<Slice> keyComparator;

  // TODO(ricebin): use fastutil or roll custom
  private final List<Integer> restarts;

  private final AtomicBoolean finished = new AtomicBoolean(false);
  private final Slice.Factory.ReusableSink blockBuf;
  private Slice prevKey;
  private int restartCounter = 0;

  PrefixBlockBuilder(Slice.Factory sliceFactory, Comparator<Slice> keyComparator) {
    this.sliceFactory = sliceFactory;
    this.keyComparator = keyComparator;
    this.prevKey = sliceFactory.empty();

    this.blockBuf = sliceFactory.newDynamicSink(BLOCK_SIZE);
    this.restarts = new ArrayList<>();
    // First restart point is at offset 0
    restarts.add(0);
  }

  @Override
  public void add(Slice key, Slice value) {
    // do not allow duplicate key
    checkArgument(keyComparator.compare(key, prevKey) > 0);

    final int sharedKeySize;
    if (restartCounter < RESTART_INTERVAL) {
      // See how much sharing to do with previous string
      sharedKeySize = SliceUtils.sharedKeySize(prevKey, key);
    } else {
      sharedKeySize = 0;
      restarts.add(blockBuf.position());
      restartCounter = 0;
    }

    int unsharedKeySize = key.len() - sharedKeySize;

    blockBuf.putVarInt(sharedKeySize);
    blockBuf.putVarInt(unsharedKeySize);
    blockBuf.putVarInt(value.len());

    blockBuf.putSlice(key.slice(sharedKeySize, unsharedKeySize));
    blockBuf.putSlice(value);

    restartCounter++;
    prevKey = key;
  }

  int getCurrentSizeEstimate() {
    return blockBuf.currentSize();
  }

  @Override
  public Slice finish() {
    checkState(finished.compareAndSet(false, true));
    for (int restart : restarts) {
      blockBuf.putInt(restart);
    }
    blockBuf.putInt(restarts.size());
    return blockBuf.finish();
  }

  @Override
  public void reset() {
    restarts.clear();
    // First restart point is at offset 0
    restarts.add(0);
    restartCounter = 0;
    blockBuf.reset();
    prevKey = null;

    checkState(finished.compareAndSet(true, false));
  }

  @Override
  public boolean isEmpty() {
    return blockBuf.position() == 0;
  }

}
