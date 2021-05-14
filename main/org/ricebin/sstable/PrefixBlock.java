package org.ricebin.sstable;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Function;
import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory;
import org.ricebin.slice.Slice.Reader;

/**
 *
 * document format
 */
// https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/doc/table_format.md
class PrefixBlock<V> implements Block<V> {

  private final Slice data;
  private final Comparator<Slice> keyComparator;
  private final Function<Slice, V> valueDecoder;
  private final Factory factory;

  PrefixBlock(Slice.Factory factory, Slice data,
      Comparator<Slice> keyComparator, Function<Slice, V> valueDecoder) {
    this.data = data;
    this.factory = factory;
    this.keyComparator = keyComparator;
    this.valueDecoder = valueDecoder;
  }

  @Override
  public Iterator<Entry<Slice, V>> iterator() {
    return iterator(0);
  }

  @Override
  public Iterator<Entry<Slice, V>> iterator(Slice lowerBound) {
    int restartOffset = searchRestarts(lowerBound);

    // advance to the first key >= lowerbound
    PeekingIterator<Entry<Slice, V>> it =
        Iterators.peekingIterator(iterator(restartOffset));
    while (it.hasNext()
        && keyComparator.compare(it.peek().getKey(), lowerBound) < 0) {
      it.next();
    }
    return it;
  }

  private Iterator<Entry<Slice, V>> iterator(int offset) {
    int numOfRestarts = data.getInt(data.len() - 4);

    int end = data.len() - 4 * (numOfRestarts + 1);
    if (end < 0) {
      throw new IllegalStateException();
    }

    int dataLen = end - offset;
    if (dataLen < 0) {
      throw new IllegalStateException();
    }

    return new PrefixBlockIterator(data.slice(offset, dataLen).newReader());
  }

  // binary search in restart array to find the first restart >= target
  private int searchRestarts(Slice target) {
    int restartsEnd = data.len() - 4;
    int restartCount = data.getInt(restartsEnd);
    int restartsBegin = restartsEnd - restartCount * 4;

    int left = 0;
    int right = restartCount - 1;

    Reader input = data.newReader();

    int firstRestartOffset = 0;
    while (left < right) {
      int mid = (left + right + 1) / 2;
      int restartOffset = restartsBegin + mid * 4;
      int pos = firstRestartOffset + data.getInt(restartOffset);

      input.position(pos);
      {
        int sharedKeySize = input.getVarInt();
        if (sharedKeySize != 0) {
          throw new IllegalStateException();
        }
      }

      int keySize = input.getVarInt();
      input.getVarInt(); // skip over value size

      Slice key = data.slice(input.position(), keySize);
      if (keyComparator.compare(key, target) < 0) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }
    return data.getInt(restartsBegin + left * 4);
  }

  // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/block.cc#L77
  private class PrefixBlockIterator extends AbstractIterator<Entry<Slice, V>> {

    private final Reader input;

    // shared key buffer
    private byte[] keyBytes = new byte[128];

    private PrefixBlockIterator(Reader input) {
      this.input = input;
    }

    // this is similar to cpp string.resize()
    private void resizeKeyIfNecessary(int targetCapacity) {
      if (targetCapacity < keyBytes.length) {
        return;
      }
      int newSize = keyBytes.length;
      while (newSize < targetCapacity) {
        newSize <<=1;
        // in case of overflow
        Preconditions.checkState(newSize > 0);
      }

      byte[] old = keyBytes;
      byte[] newKey = new byte[newSize];
      System.arraycopy(old, 0, newKey, 0, old.length);
      keyBytes = newKey;
    }

    @Override
    protected Entry<Slice, V> computeNext() {
      if (input.position() == input.len()) {
        return super.endOfData();
      }

      int sharedKeySize = input.getVarInt();
      int unsharedKeySize = input.getVarInt();
      int valSize = input.getVarInt();

      int keySize = sharedKeySize + unsharedKeySize;
      resizeKeyIfNecessary(keySize);

      input.getBytes(keyBytes, sharedKeySize, unsharedKeySize);

      Slice value = input.slice(valSize);

      input.position(input.position() + valSize);

      Slice keySlice = factory.wrap(keyBytes, 0, keySize);
      // TODO(ricebin): we can prob lazy decode value
      return new SimpleImmutableEntry<>(keySlice, valueDecoder.apply(value));
    }
  }
}
