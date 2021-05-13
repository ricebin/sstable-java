package org.ricebin.slice;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.truth.Truth;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;
import org.ricebin.slice.Slice.Factory;
import org.ricebin.slice.Slice.Factory.ReusableSink;
import org.ricebin.slice.Slice.Reader;

public class ByteBufferSliceTest {

  private Factory factory;
  private Random random;

  @Before
  public void setUp() {
    factory = ByteBufferSlice.FACTORY;
    random = new Random(1234);
  }

  @Test
  public void testVarInt32() {
    List<Integer> values = new ArrayList<>();
    values.add(Integer.MAX_VALUE);

    for (int i = 0; i < (32 * 32); i++) {
      int v = Math.abs(random.nextInt());
      values.add(v);
    }

    ReusableSink sink = factory.newDynamicSink(1000);
    for (int v : values) {
      sink.putVarInt(v);
    }
    Slice.Reader reader = sink.finish().newReader();

    for (int expected : values) {
      int actual = reader.getVarInt();
      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void testVarInt64() {
    int len = 32 * 32;
    List<Long> values = new ArrayList<>(len + 1);
    values.add(Long.MAX_VALUE);

    for (int i = 0; i < len; i++) {
      long v = Math.abs(random.nextLong());
      values.add(v);
    }

    ReusableSink sink = factory.newDynamicSink(1000);
    for (long v : values) {
      sink.putVarLong(v);
    }

    Slice slice = sink.finish();
    Slice.Reader reader = slice.newReader();
    for (long expected : values) {
      assertThat(reader.getVarLong()).isEqualTo(expected);
    }
  }

  @Test
  public void testInt32() {
    List<Integer> values = new ArrayList<>();
    values.add(Integer.MAX_VALUE);

    for (int i = 0; i < (32 * 32); i++) {
      int v = Math.abs(random.nextInt());
      values.add(v);
    }

    int expectedSize = 0;
    ReusableSink sink = factory.newDynamicSink(1000);
    for (int v : values) {
      sink.putInt(v);
      expectedSize = expectedSize + Slice.SIZE_OF_INT;
    }

    Slice slice = sink.finish();
    assertThat(slice.len()).isEqualTo(expectedSize);

    Slice.Reader reader = slice.newReader();
    for (int expected : values) {
      {
        Slice subSlice = reader.slice(Slice.SIZE_OF_INT);
        assertThat(subSlice.getInt(0)).isEqualTo(expected);
        Reader subReader = subSlice.newReader();
        assertThat(subReader.getInt()).isEqualTo(expected);
        assertThat(subReader.hasRemaining()).isFalse();
      }

      int pos = reader.position();
      {
        Slice subSlice = slice.slice(pos, Slice.SIZE_OF_INT);
        assertThat(subSlice.len()).isEqualTo(Slice.SIZE_OF_INT);

        assertThat(subSlice.getInt(0)).isEqualTo(expected);
        Reader subReader = subSlice.newReader();
        assertThat(subReader.getInt()).isEqualTo(expected);
        assertThat(subReader.hasRemaining()).isFalse();
      }
      assertThat(slice.getInt(pos)).isEqualTo(expected);
      assertThat(reader.getInt()).isEqualTo(expected);

    }
  }

  @Test
  public void testLong() {
    List<Long> values = new ArrayList<>();
    values.add(Long.MAX_VALUE);

    for (int i = 0; i < (32 * 32); i++) {
      long v = Math.abs(random.nextLong());
      values.add(v);
    }

    ReusableSink sink = factory.newDynamicSink(1000);
    int expectedSize = 0;
    for (long v : values) {
      sink.putLong(v);
      expectedSize = expectedSize + Slice.SIZE_OF_LONG;
      Truth.assertThat(sink.currentSize()).isEqualTo(expectedSize);
    }

    Slice slice = sink.finish();
    assertThat(slice.len()).isEqualTo(expectedSize);
    Slice.Reader reader = slice.newReader();
    for (long expected : values) {
      {
        Slice subSlice = reader.slice(Slice.SIZE_OF_LONG);
        assertThat(subSlice.len()).isEqualTo(Slice.SIZE_OF_LONG);

        assertThat(subSlice.getLong(0)).isEqualTo(expected);
        Reader subReader = subSlice.newReader();
        assertThat(subReader.getLong()).isEqualTo(expected);
        assertThat(subReader.hasRemaining()).isFalse();
      }
      int pos = reader.position();
      {
        Slice subSlice = slice.slice(pos, Slice.SIZE_OF_LONG);
        assertThat(subSlice.getLong(0)).isEqualTo(expected);
        Reader subReader = subSlice.newReader();
        assertThat(subReader.getLong()).isEqualTo(expected);
        assertThat(subReader.hasRemaining()).isFalse();
      }
      assertThat(slice.getLong(pos)).isEqualTo(expected);
      assertThat(reader.getLong()).isEqualTo(expected);
    }
  }

}