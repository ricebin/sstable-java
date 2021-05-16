package org.ricebin.sstable;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import org.ricebin.slice.Slice;

public class Table {

  private final FileChannel fileChannel;
  private final Slice.Factory sliceFactory;
  final Block<Slice> index;

  Table(FileChannel fileChannel, Slice.Factory sliceFactory, Block<Slice> index) {
    this.fileChannel = fileChannel;
    this.sliceFactory = sliceFactory;
    this.index = index;
  }

  public Iterator<Entry<Slice, Slice>> iterator(Slice key) {
    return index.iterator(key);
  }

  public Iterator<Map.Entry<Slice, Slice>> iterator() {
    return index.iterator();
  }

  public static Table open(
      FileChannel fileChannel, Slice.Factory sliceFactory) throws IOException {
    Comparator<Slice> keyComparator = sliceFactory.comparator();
    Footer footer = readFooter(sliceFactory, fileChannel);

    PrefixBlock<BlockHandle> blockIndex = readBlock(
        sliceFactory,
        fileChannel, footer.getIndex(), keyComparator,
        valueSlice -> BlockHandle.decode(valueSlice.newReader()));

    // TODO(ricebin): impl metaIndex
    // PrefixBlock metaIndex = readBlock(sliceFactory, fileChannel, footer.getMetaIndex(), keyComparator);
    return new Table(
        fileChannel,
        sliceFactory,
        new TwoLevelBlock<>(blockIndex,
            blockHandle -> {
              try {
                return readBlock(sliceFactory, fileChannel, blockHandle, keyComparator, s -> s);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }));
  }

  static <V> PrefixBlock<V> readBlock(
      Slice.Factory sliceFactory, FileChannel fileChannel, BlockHandle blockHandle,
      Comparator<Slice> keyComparator, Function<Slice, V> valueDecoder) throws IOException{

    // read block trailer
    long trailerStart = blockHandle.getOffset() + blockHandle.getSize();
    Slice trailerSlice = sliceFactory.readFully(fileChannel, trailerStart, BlockTrailer.MAX_ENCODED_LENGTH);

    BlockTrailer trailer = BlockTrailer.decode(trailerSlice);
    // TODO(ricebin): verify checksum from trailer

    Slice dataSlice = sliceFactory.readFully(fileChannel, blockHandle.getOffset(), blockHandle.getSize());
    return new PrefixBlock<V>(sliceFactory, dataSlice, keyComparator, valueDecoder);
  }

  static Footer readFooter(Slice.Factory sliceFactory, FileChannel fileChannel) throws IOException {
    int size = Ints.checkedCast(fileChannel.size());
    long footerOffset = size - Footer.MAX_ENCODED_LENGTH;
    Slice slice = sliceFactory.readFully(fileChannel, footerOffset, Footer.MAX_ENCODED_LENGTH);
    return Footer.decode(slice);
  }

  public void close() throws IOException {
    fileChannel.close();
  }
}