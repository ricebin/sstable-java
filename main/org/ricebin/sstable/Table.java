package org.ricebin.sstable;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.ricebin.slice.Slice;

public class Table {

  private final ReadOnlyFile inputFile;
  private final Slice.Factory sliceFactory;

  private final Block<BlockHandle> blockIndex;
  private final Function<BlockHandle, Block<Slice>> getBlock;
  final FilterBlock filterBlock;

  Table(
      ReadOnlyFile inputFile,
      Slice.Factory sliceFactory,
      Block<BlockHandle> blockIndex,
      FilterBlock filterBlock) {
    this.inputFile = inputFile;
    this.sliceFactory = sliceFactory;
    this.blockIndex = blockIndex;
    this.filterBlock = filterBlock;
    this.getBlock =
        blockHandle -> {
          try {
            return readBlock(sliceFactory, inputFile, blockHandle, s -> s);
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

    PrefixBlock<Slice> valueBlock = readBlock(sliceFactory, inputFile, valueBlockHandle, s -> s);
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

  public static Table openWithoutFilter(
      File file,
      Slice.Factory sliceFactory) throws IOException {
    return open(file, null, sliceFactory);
  }


  public static Table open(
      File file,
      FilterPolicy.Reader filterPolicy,
      Slice.Factory sliceFactory) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    return open(filterPolicy, new FileChannelReadOnlyFile(randomAccessFile.getChannel()), sliceFactory);
  }

  public static Table open(
      FilterPolicy.Reader filterPolicy,
      ReadOnlyFile inputFile,
      Slice.Factory sliceFactory) throws IOException {
    Footer footer = readFooter(sliceFactory, inputFile);

    PrefixBlock<BlockHandle> blockIndex = readBlock(
        sliceFactory,
        inputFile, footer.getIndex(),
        valueSlice -> BlockHandle.decode(valueSlice.newReader()));

    FilterBlock filterBlock;
    if (filterPolicy != null) {

      // https://github.com/google/leveldb/blob/f57513a1d6c99636fc5b710150d0b93713af4e43/table/table.cc#L82
      filterBlock = readFilterBlock(sliceFactory, filterPolicy, inputFile, footer.getMetaIndex());
    } else {
      filterBlock = null;
    }

    return new Table(
        inputFile,
        sliceFactory,
        blockIndex,
        filterBlock);
  }

  static FilterBlock readFilterBlock(
      Slice.Factory sliceFactory,
      FilterPolicy.Reader filterPolicy,
      ReadOnlyFile file,
      BlockHandle metaIndexBlockHandle)
      throws IOException {
    PrefixBlock<Slice> metaIndex = readBlock(
        sliceFactory,
        file,
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

        ByteBuffer blockBuf = file.readFully(filterBlockHandle.getOffset(), filterBlockHandle.getSize());

        Slice filterBlockData = sliceFactory.wrap(blockBuf);

        return FilterBlock.newInstance(filterBlockData, filterPolicy);
      }
    }
    return null;
  }

  static <V> PrefixBlock<V> readBlock(
      Slice.Factory sliceFactory, ReadOnlyFile file, BlockHandle blockHandle,
      Function<Slice, V> valueDecoder) throws IOException {

    // read block data + trailer
    ByteBuffer dataAndTrailer = file.readFully(
        blockHandle.getOffset(),
        blockHandle.getSize() + BlockTrailer.MAX_ENCODED_LENGTH);

    BlockTrailer blockTrailer = BlockTrailer.decode(
        SliceUtils.duplicate(dataAndTrailer).position(blockHandle.getSize()));

    Compressor compressor = blockTrailer.getCompressionType().getCompressor();
    ByteBuffer raw = dataAndTrailer.limit(blockHandle.getSize());
    ByteBuffer uncompressed = compressor.uncompress(raw);

    Slice dataSlice = sliceFactory.wrap(uncompressed);

    return new PrefixBlock<V>(sliceFactory, dataSlice, sliceFactory.comparator(), valueDecoder);
  }

  static Footer readFooter(Slice.Factory sliceFactory, ReadOnlyFile file) throws IOException {
    int size = Ints.checkedCast(file.size());
    long footerOffset = size - Footer.MAX_ENCODED_LENGTH;
    Slice slice = sliceFactory.wrap(
        file.readFully(footerOffset, Footer.MAX_ENCODED_LENGTH));
    return Footer.decode(slice);
  }

  public void close() throws IOException {
    inputFile.close();
  }
}