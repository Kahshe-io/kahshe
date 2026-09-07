package io.kahshe.proxy.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PlanServiceTest {

  @Test
  void taskWeightBytesWithNoStats() {
    String location = "s3://bucket/data/file-1.parquet";
    assertEquals(
        384 + 2L * location.length(),
        PlanService.taskWeightBytes(location, null, null, null, null, 0));
  }

  @Test
  void taskWeightBytesCountsStatsBoundsAndDeletes() {
    String location = "file-2.parquet";
    Map<Integer, Long> valueCounts = Map.of(1, 10L, 2, 20L);
    Map<Integer, Long> nullCounts = Map.of(1, 0L);
    Map<Integer, ByteBuffer> lower = Map.of(1, ByteBuffer.allocate(4));
    Map<Integer, ByteBuffer> upper = Map.of(1, ByteBuffer.allocate(6));
    // statCols is the max of the stat maps' sizes (2), bounds add their payload bytes
    long expected = 384 + 2L * location.length() + 320L * 2 + 4 + 6 + 96L * 3;
    assertEquals(
        expected,
        PlanService.taskWeightBytes(location, valueCounts, nullCounts, lower, upper, 3));
  }

  @Test
  void taskWeightBytesToleratesNullBuffers() {
    String location = "file-3.parquet";
    Map<Integer, ByteBuffer> bounds = new HashMap<>();
    bounds.put(1, null);
    assertEquals(
        384 + 2L * location.length() + 320L,
        PlanService.taskWeightBytes(location, null, null, bounds, null, 0));
  }
}
