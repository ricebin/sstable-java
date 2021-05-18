package org.ricebin.sstable;

import com.google.common.hash.Hashing;
import java.util.Iterator;
import java.util.Map.Entry;
import org.ricebin.slice.Slice;

// https://github.com/google/leveldb/blob/master/table/filter_block.cc
class FilterBlock {

  final Slice blockContent;
  private final int baseLg;
  private final Slice offsets;
  private final int num;
  private final FilterPolicy.Reader filterPolicy;

  private FilterBlock(Slice blockContent, Slice offsets, FilterPolicy.Reader filterPolicy) {
    this.baseLg = blockContent.getByte(blockContent.len() - 1);
    this.blockContent = blockContent;
    this.offsets = offsets;
    // make sure this is multiple of 4
    this.num = offsets.len() / 4;
    this.filterPolicy = filterPolicy;
  }

  boolean mayExists(long blockOffset, Slice key) {
    long index = blockOffset >> baseLg;
    if (index < num) {
      int start = offsets.getInt((int) (index * 4));
      int limit = offsets.getInt((int) (index * 4 + 4));
      if (start == limit) {
        // Empty filters do not match any keys
        return false;
      } else {
        Slice filterSlice = blockContent.slice(start, limit - start);
        return filterPolicy.keyMayMatch(key, filterSlice);
      }
    }
    return true;  // Errors are treated as potential matches
  }

  static FilterBlock newInstance(Slice contents, FilterPolicy.Reader filterPolicy) {
    int size = contents.len();
    if (size < 5) {
      // 1 byte for base_lg_ and 4 for start of offset array
      return null;
    }
    int lastWord = contents.getInt(size - 5);
    if (lastWord > size - 5) {
      return null;
    }
    int dataLength = size - 5 - lastWord;
    return new FilterBlock(contents, contents.slice(lastWord, dataLength), filterPolicy);
  }
}
