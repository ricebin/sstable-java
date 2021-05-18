package org.ricebin.sstable;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;
import org.ricebin.slice.ByteBufferSlice;
import org.ricebin.slice.Slice;
import org.ricebin.sstable.FilterPolicy.Writer;

// https://github.com/google/leveldb/blob/master/util/bloom_test.cc
public class BloomFilterPolicyTest {

  private BloomFilterPolicy policy;

  @Before
  public void setUp() {
    policy = BloomFilterPolicy.LEVELDB_BUILTIN_BLOOM_FILTER2;
  }

  @Test
  public void testEmpty() {
    Writer writer = policy.getWriter(10);
    Slice filter = newSlice(writer.createFilter(ImmutableSet.of()));
    assertThat(policy.getReader().keyMayMatch(newSlice("hello"), filter)).isFalse();
    assertThat(policy.getReader().keyMayMatch(newSlice("world"), filter)).isFalse();
  }

  @Test
  public void testHappy() {
    Writer writer = policy.getWriter(10);
    Slice filter = newSlice(writer.createFilter(ImmutableSet.of(
        newSlice("hello"),
        newSlice("world"))));
    assertThat(policy.getReader().keyMayMatch(newSlice("hello"), filter)).isTrue();
    assertThat(policy.getReader().keyMayMatch(newSlice("world"), filter)).isTrue();
    assertThat(policy.getReader().keyMayMatch(newSlice("x"), filter)).isFalse();
    assertThat(policy.getReader().keyMayMatch(newSlice("foo"), filter)).isFalse();

  }

  static Slice newSlice(String v) {
    return newSlice(v.getBytes(StandardCharsets.UTF_8));
  }

  static Slice newSlice(byte[] b) {
    return ByteBufferSlice.FACTORY.wrap(b, 0, b.length);
  }


}