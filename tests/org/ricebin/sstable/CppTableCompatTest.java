package org.ricebin.sstable;


import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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
  public void testHappy() throws IOException {
    Slice firstKey = newSlice(new byte[] {48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 1, 1, 0, 0, 0, 0, 0, 0});
    Slice midKey = newSlice(new byte[] {48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 52, 50, 1, 43, 0, 0, 0, 0, 0, 0});

    RandomAccessFile file = new RandomAccessFile(
        "tests/org/ricebin/sstable/testfiles/000005.sst", "r");
    FileChannel fc = file.getChannel();

    Table table = Table.open(fc, ByteBufferSlice.FACTORY);
    {
      Iterator<Entry<Slice, Slice>> it = table.iterator();
      assertThat(it.hasNext()).isTrue();
      assertThat(equals(it.next().getKey(), firstKey)).isTrue();
    }

    {
      Iterator<Entry<Slice, Slice>> it = table.iterator(midKey);
      assertThat(it.hasNext()).isTrue();
      assertThat(equals(it.next().getKey(), midKey)).isTrue();
    }

    {
      Random random = new Random();
      int pos = random.nextInt(10000);
      Entry<Slice, Slice> nthEntry = getEntry(table, pos);
      {
        Entry<Slice, Slice> entry = getNext(table, nthEntry.getKey());
        assertThat(equals(entry.getKey(), nthEntry.getKey())).isTrue();
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

  private static boolean equals(Slice a, Slice b) {
    if (a.len() != b.len()) {
      return false;
    }

    for (int i = 0; i < a.len(); i++) {
      if (a.getByte(i) != b.getByte(i)) {
        return false;
      }
    }
    return true;
  }
}
