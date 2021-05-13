package org.ricebin.sstable;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.ricebin.slice.ByteBufferSlice;
import org.ricebin.slice.BytewiseSliceComparator;
import org.ricebin.slice.Slice;

public class TableTest {

  private static final Slice.Factory SLICE_FACTORY = ByteBufferSlice.FACTORY;
  private static final BytewiseSliceComparator KEY_COMPARATOR = BytewiseSliceComparator.INSTANCE;

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testHappy() throws IOException {
    assertTable(ImmutableMap.of("a", "avalue"));

    // shared key
    assertTable(ImmutableMap.of(
        "a", "avalue",
        "aa", "aavalue"));

  }

  private void assertTable(ImmutableMap<String, String> input) throws IOException {
    File file = writeTable(input);

    Table table = readTable(file);
    ImmutableMap<String, String> map = toMap(table);

    assertThat(map).containsExactlyEntriesIn(input).inOrder();
  }

  private static ImmutableMap<String, String> toMap(Table table) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    Iterator<Entry<Slice, Slice>> it = table.iterator();
    while (it.hasNext()) {
      Entry<Slice, Slice> next = it.next();
      builder.put(asString(next.getKey()), asString(next.getValue()));
    }
    return builder.build();
  }

  private Table readTable(File file) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    return Table.open(randomAccessFile.getChannel(), KEY_COMPARATOR, SLICE_FACTORY);
  }

  private File writeTable(ImmutableMap<String, String> input) throws IOException {
    File file = tempDir.newFile();

    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
    FileChannel fc = randomAccessFile.getChannel();

    TableBuilder builder =
        new TableBuilder(SLICE_FACTORY, fc, KEY_COMPARATOR, CompressionType.NONE);

    for (Map.Entry<String, String> entry : input.entrySet()) {
      builder.add(newSlice(entry.getKey()), newSlice(entry.getValue()));
    }

    builder.finish();

    return file;
  }

  static String asString(Slice slice) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(slice.len());
    Slice.Reader reader = slice.newReader();
    while (reader.hasRemaining()) {
      out.write(reader.getByte());
    }
    return new String(out.toByteArray(), Charsets.UTF_8);
  }

  static Slice newSlice(String input) {
    byte[] bytes = input.getBytes(Charsets.UTF_8);
    return SLICE_FACTORY.wrap(bytes, 0, bytes.length);
  }

}
