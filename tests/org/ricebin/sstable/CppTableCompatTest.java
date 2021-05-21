package org.ricebin.sstable;


import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.ricebin.slice.ByteBufferSlice;
import org.ricebin.slice.Slice;

public class CppTableCompatTest {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testWithFilter() throws Exception {
    String fileName =
        "tests/org/ricebin/sstable/testfiles/testWithFilter.sst";
    Table table = Table.open(
        new File(fileName),
        BloomFilterPolicy.LEVELDB_BUILTIN_BLOOM_FILTER2.getReader(),
        ByteBufferSlice.FACTORY);

    assertThat(getBytes(table.filterBlock.blockContent))
        .isEqualTo(new byte[]{0, 8, 64, 2, 16, 0, 4, 32, 6, 0, 0, 0, 0, 9, 0, 0, 0, 11});

    assertThat(table.mayExists(newSlice("key1"))).isTrue();
    assertThat(getBytes(table.get(newSlice("key1")))).isEqualTo("value2".getBytes());

    assertThat(table.mayExists(newSlice("key2"))).isTrue();
    assertThat(table.get(newSlice("key2"))).isNull();

    assertThat(table.mayExists(newSlice("key3"))).isTrue();
    assertThat(getBytes(table.get(newSlice("key3")))).isEqualTo("value4".getBytes());

    assertThat(table.mayExists(newSlice("key123"))).isFalse();
    assertThat(table.get(newSlice("key123"))).isNull();
  }

  @Test
  public void test_openWithoutFilter() throws IOException {
    Slice firstKey = newSlice(
        new byte[]{48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 1, 1, 0, 0, 0, 0,
            0, 0});
    Slice midKey = newSlice(
        new byte[]{48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 52, 50, 1, 43, 0, 0, 0,
            0, 0, 0});

    String filename = "tests/org/ricebin/sstable/testfiles/000005.sst";
    Table table = Table.openWithoutFilter(new File(filename), ByteBufferSlice.FACTORY);
    {
      Iterator<Entry<Slice, Slice>> it = table.iterator();
      assertThat(it.hasNext()).isTrue();
      assertThat(getBytes(it.next().getKey())).isEqualTo(getBytes(firstKey));
    }

    {
      Iterator<Entry<Slice, Slice>> it = table.iterator(midKey);
      assertThat(it.hasNext()).isTrue();
      assertThat(getBytes(it.next().getKey())).isEqualTo(getBytes(midKey));
    }

    {
      Random random = new Random();
      int pos = random.nextInt(10000);
      Entry<Slice, Slice> nthEntry = getEntry(table, pos);
      {
        Entry<Slice, Slice> entry = getNext(table, nthEntry.getKey());
        assertThat(getBytes(entry.getKey())).isEqualTo(getBytes(nthEntry.getKey()));
      }
    }
  }

  private static Map.Entry<Slice, Slice> getNext(Table table, Slice key) {
    Iterator<Entry<Slice, Slice>> it = table.iterator(key);
    return it.next();
  }

  private static Map.Entry<Slice, Slice> getEntry(Table table, int pos) {
    Iterator<Entry<Slice, Slice>> it = table.iterator();
    Iterators.advance(it, pos);
    return it.next();
  }

  private static Slice newSlice(byte[] bytes) {
    return ByteBufferSlice.FACTORY.wrap(bytes, 0, bytes.length);
  }

  private static Slice newSlice(String v) {
    return newSlice(v.getBytes(StandardCharsets.UTF_8));
  }


  private static byte[] getBytes(Slice slice) {
    byte[] sink = new byte[slice.len()];
    slice.newReader().getBytes(sink, 0, slice.len());
    return sink;
  }

}
