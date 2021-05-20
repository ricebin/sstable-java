package org.ricebin.sstable;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.ricebin.slice.Slice;

public class Table {

  private final FileChannel fileChannel;
  private final Slice.Factory sliceFactory;

  private final Block<BlockHandle> blockIndex;
  private final Function<BlockHandle, Block<Slice>> getBlock;
  final FilterBlock filterBlock;

  Table(
      FileChannel fileChannel,
      Slice.Factory sliceFactory,
      Block<BlockHandle> blockIndex,
      FilterBlock filterBlock) {
    this.fileChannel = fileChannel;
    this.sliceFactory = sliceFactory;
    this.blockIndex = blockIndex;
    this.filterBlock = filterBlock;
    this.getBlock =
        blockHandle -> {
          try {
            return readBlock(sliceFactory, fileChannel, blockHandle, s -> s);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
  }

  boolean mayExists(Slice key) {
    return getValueBlock(key) != null;
  }

  private BlockHandle getValueBlock(Slice key) {
    Iterator<Entry<Slice, BlockHandle>> blockIt = blockIndex.iterator(key);
    if (!blockIt.hasNext()) {
      return null;
    }

    BlockHandle blockHandle = blockIt.next().getValue();
    if (filterBlock != null && !filterBlock.mayExists(blockHandle.getOffset(), key)) {
      return null;
    }
    return blockHandle;
  }

  public Slice get(Slice key) throws IOException {
    BlockHandle valueBlockHandle = getValueBlock(key);
    if (valueBlockHandle == null) {
      return null;
    }

    PrefixBlock<Slice> valueBlock = readBlock(sliceFactory, fileChannel, valueBlockHandle, s -> s);
    Iterator<Entry<Slice, Slice>> valueIt = valueBlock.iterator(key);
    if (valueIt.hasNext()) {
      Entry<Slice, Slice> next = valueIt.next();
      if (sliceFactory.comparator().compare(key, next.getKey()) == 0) {
        return next.getValue();
      }
    }
    return null;
  }

  public Iterator<Entry<Slice, Slice>> iterator(Slice lowerBound) {
    return Iterators.concat(
        Iterators.transform(
            blockIndex.iterator(lowerBound),
            e -> getBlock.apply(e.getValue()).iterator(lowerBound)));
  }

  public Iterator<Map.Entry<Slice, Slice>> iterator() {
    return Iterators.concat(
        Iterators.transform(
            blockIndex.iterator(),
            e -> getBlock.apply(e.getValue()).iterator()));
  }

  public static Table open(
      FileChannel fileChannel,
      Slice.Factory sliceFactory) throws IOException {
    return open(null, fileChannel, sliceFactory);
  }

  public static Table open(
      FilterPolicy.Reader filterPolicy,
      FileChannel fileChannel,
      Slice.Factory sliceFactory) throws IOException {
    Footer footer = readFooter(sliceFactory, fileChannel);

    PrefixBlock<BlockHandle> blockIndex = readBlock(
        sliceFactory,
        fileChannel, footer.getIndex(),
        valueSlice -> BlockHandle.decode(valueSlice.newReader()));

    FilterBlock filterBlock;
    if (filterPolicy != null) {

      // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/table.cc#L82
      filterBlock = readFilterBlock(sliceFactory, filterPolicy, fileChannel, footer.getMetaIndex());
    } else {
      filterBlock = null;
    }

    return new Table(
        fileChannel,
        sliceFactory,
        blockIndex,
        filterBlock);
  }

  static FilterBlock readFilterBlock(
      Slice.Factory sliceFactory,
      FilterPolicy.Reader filterPolicy,
      FileChannel fileChannel,
      BlockHandle metaIndexBlockHandle)
      throws IOException {
    PrefixBlock<Slice> metaIndex = readBlock(
        sliceFactory,
        fileChannel,
        metaIndexBlockHandle,
        s -> s
    );

    byte[] filterKeyBytes = ("filter." + filterPolicy.name()).getBytes(StandardCharsets.UTF_8);
    Slice filterKeySlice = sliceFactory.wrap(filterKeyBytes, 0, filterKeyBytes.length);

    Iterator<Entry<Slice, Slice>> entries =
        metaIndex.iterator(sliceFactory.wrap(filterKeyBytes, 0, filterKeyBytes.length));
    if (entries.hasNext()) {
      Entry<Slice, Slice> next = entries.next();
      if (sliceFactory.comparator().compare(filterKeySlice, next.getKey()) == 0) {
        Slice data = next.getValue();

        BlockHandle filterBlockHandle = BlockHandle.decode(data.newReader());

        ByteBuffer blockBuf = SliceUtils
            .readFully(fileChannel, filterBlockHandle.getOffset(), filterBlockHandle.getSize());

        Slice filterBlockData = sliceFactory.wrap(blockBuf);

        return FilterBlock.newInstance(filterBlockData, filterPolicy);
      }
    }
    return null;
  }

  static <V> PrefixBlock<V> readBlock(
      Slice.Factory sliceFactory, FileChannel fileChannel, BlockHandle blockHandle,
      Function<Slice, V> valueDecoder) throws IOException {

    // read block trailer
    long trailerStart = blockHandle.getOffset() + blockHandle.getSize();
    Slice trailerSlice = sliceFactory.wrap(
        SliceUtils.readFully(fileChannel, trailerStart, BlockTrailer.MAX_ENCODED_LENGTH));

    BlockTrailer trailer = BlockTrailer.decode(trailerSlice);
    // TODO(ricebin): verify checksum from trailer

    ByteBuffer blockBuf = SliceUtils
        .readFully(fileChannel, blockHandle.getOffset(), blockHandle.getSize());

    Compressor compressor = trailer.getCompressionType().getCompressor();
    ByteBuffer uncompressed = compressor.uncompress(blockBuf);

    Slice dataSlice = sliceFactory.wrap(uncompressed);

    return new PrefixBlock<V>(sliceFactory, dataSlice, sliceFactory.comparator(), valueDecoder);
  }

  static Footer readFooter(Slice.Factory sliceFactory, FileChannel fileChannel) throws IOException {
    int size = Ints.checkedCast(fileChannel.size());
    long footerOffset = size - Footer.MAX_ENCODED_LENGTH;
    Slice slice = sliceFactory.wrap(
        SliceUtils.readFully(fileChannel, footerOffset, Footer.MAX_ENCODED_LENGTH));
    return Footer.decode(slice);
  }

  public void close() throws IOException {
    fileChannel.close();
  }
}