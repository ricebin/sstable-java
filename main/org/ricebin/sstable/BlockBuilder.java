package org.ricebin.sstable;


import org.ricebin.slice.Slice;

// https://github.com/google/leveldb/blob/master/table/block_builder.h
interface BlockBuilder {

  void add(Slice key, Slice value);

  Slice finish();

  void reset();

  boolean isEmpty();
}
