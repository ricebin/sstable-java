package org.ricebin.sstable.benchmark;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.ricebin.slice.ByteBufferSlice;
import org.ricebin.slice.Slice;
import org.ricebin.slice.Slice.Factory.Sink;
import org.ricebin.sstable.BloomFilterPolicy;
import org.ricebin.sstable.Table;

public class RandomGetBenchmark {
  @State(Scope.Benchmark)
  public static class MyState {
    private Random random;
    private Table table;
    private Slice key;

    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
      random = new Random();
      RandomAccessFile file = new RandomAccessFile(
          "benchmark/org/ricebin/sstable/benchmark/testfiles/000005.sst", "r");
      FileChannel fc = file.getChannel();

//      table = Table.open(fc, ByteBufferSlice.FACTORY);
      table = Table.open(BloomFilterPolicy.INSTANCE.getReader(), fc, ByteBufferSlice.FACTORY);
    }

    @TearDown
    public void tearDown() throws Exception {
      table.close();
    }

    @Setup(Level.Invocation)
    public void setUpRandomKey() throws Exception {
//      byte[] sampleKey = new byte[]{
//          48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 51, 51, 55, 49};
      int fullkeySize = 24;
      Sink keySink = ByteBufferSlice.FACTORY.newFixedSizeSink(24 + 4);
      for (int i = 0; i < 10; i++) {
        keySink.putByte((byte) 48);
      }
      for (int i = 0; i < 14; i++) {
        keySink.putByte((byte) random.nextInt(100));
      }
      key = keySink.finish();
    }
  }

  @Benchmark
  public void testIteratorNext(MyState state) {
    Iterator<Entry<Slice, Slice>> it = state.table.iterator(state.key);
    boolean found = it.hasNext();
    if (found) {
      Entry<Slice, Slice> next = it.next();
    }
//    System.out.println("found: " + found);
  }

  @Benchmark
  public void testGet(MyState state) throws Exception {
    Slice value = state.table.get(state.key);
  }

}
