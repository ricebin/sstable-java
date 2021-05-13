package org.ricebin.sstable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory;

public class TableBuilder {


  private final Slice.Factory sliceFactory;
  private final FileChannel fileChannel;
  private final Comparator<Slice> keyComparator;
  private final CompressionType compressionType;

  private final PrefixBlockBuilder dataBlockBuilder;
  private final PrefixBlockBuilder indexBlockBuilder;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  private Slice prevKey;
  private BlockHandle pendingIndexEntry = null;

  TableBuilder(Slice.Factory sliceFactory,
      FileChannel fileChannel, Comparator<Slice> keyComparator, CompressionType compressionType) {
    checkArgument(compressionType == CompressionType.NONE);
    this.sliceFactory = sliceFactory;
    this.fileChannel = fileChannel;
    this.keyComparator = keyComparator;
    this.compressionType = compressionType;
    this.prevKey = sliceFactory.empty();

    this.dataBlockBuilder = new PrefixBlockBuilder(sliceFactory, keyComparator);
    this.indexBlockBuilder = new PrefixBlockBuilder(sliceFactory, keyComparator);
  }

  public void add(Slice key, Slice value) throws IOException {
    // do not allow duplicate key
    checkState(keyComparator.compare(key, prevKey) > 0);

    if (pendingIndexEntry != null) {
      checkState(dataBlockBuilder.isEmpty());

      // TODO(ricebin): optimize with findShortestSeparator
      // keyComparator.findShortestSeparator(prevKey, key);

      Slice handle = BlockHandle.encode(pendingIndexEntry, sliceFactory);
      indexBlockBuilder.add(prevKey, handle);
      pendingIndexEntry = null;
    }

    prevKey = key;
    dataBlockBuilder.add(key, value);

    int estimatedBlockSize = dataBlockBuilder.getCurrentSizeEstimate();
    if (estimatedBlockSize >= PrefixBlockBuilder.BLOCK_SIZE) {
      flush();
    }
  }

  private BlockHandle writeBlock(BlockBuilder blockBuilder) throws IOException {
    // close the block
    Slice blockBuffer = blockBuilder.finish();

    // TODO(ricebin): compress the block

    // TODO(ricebin): crc32c
    int crc32c = 0;

    // create block trailer
    BlockTrailer blockTrailer = new BlockTrailer(compressionType, crc32c);
    Slice trailerBuf = BlockTrailer.encode(blockTrailer, sliceFactory);

    // create a handle to this block
    long pos = fileChannel.position();
    BlockHandle blockHandle = new BlockHandle(pos, blockBuffer.len());

    blockBuffer.newReader().write(fileChannel);
    trailerBuf.newReader().write(fileChannel);

    blockBuilder.reset();

    return blockHandle;
  }

  private void flush() throws IOException {
    if (dataBlockBuilder.isEmpty()) {
      return;
    }

    checkState(pendingIndexEntry == null);

    pendingIndexEntry = writeBlock(dataBlockBuilder);
  }

  public void finish() throws IOException {
    checkState(finished.compareAndSet(false, true));

    flush();

    // TODO(ricebin): Write filter block

    // write meta index block
    // TODO(ricebin): actually write something here
    BlockBuilder metaIndexBlockBuilder = new PrefixBlockBuilder(sliceFactory, keyComparator);
    // TODO(postrelease): Add stats and other meta blocks
    BlockHandle metaIndexBlockHandle = writeBlock(metaIndexBlockBuilder);

    // Write index block
    // add last handle to index block
    if (pendingIndexEntry != null) {
      checkState(dataBlockBuilder.isEmpty());

      Slice handleEncoding = BlockHandle.encode(pendingIndexEntry, sliceFactory);
      indexBlockBuilder.add(prevKey, handleEncoding);
      pendingIndexEntry = null;
    }
    BlockHandle indexBlockHandle = writeBlock(indexBlockBuilder);

    // write footer
    Footer footer = new Footer(metaIndexBlockHandle, indexBlockHandle);
    Slice footerBuf = Footer.encode(footer, sliceFactory);
    footerBuf.newReader().write(fileChannel);
  }
}
