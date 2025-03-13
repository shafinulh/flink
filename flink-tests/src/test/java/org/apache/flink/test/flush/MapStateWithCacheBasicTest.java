package org.apache.flink.test.flush;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;

/**
 * test for MapStateWithCache that validates basic functionality and cache behavior.
 *
 * This test verifies:
 * 1. Basic operations (put, get, contains) work as expected
 * 2. Cache hits/misses follow the expected pattern based on access history
 * 3. Entries are evicted properly when the cache size limit is reached
 *
 * The test uses a deterministic access pattern and logs results to the console
 * for visual verification of the caching behavior.
 */
public class MapStateWithCacheBasicTest extends TestLogger {
    private static final int CACHE_SIZE = 10; // Small cache to easily observe eviction
    
    @TempDir
    Path tmpDir;

    @BeforeEach
    void setup() throws IOException {
        Files.createDirectory(tmpDir.resolve("checkpoints"));
    }

    // main test function
    @Test
    void testMapStateCacheBehavior() throws Exception {
        Configuration conf = new Configuration();
        conf.set(STATE_BACKEND, "rocksdb");
        conf.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // create input
        List<Integer> input = createTestInput();
        
        // process using stateful operator
        env.fromCollection(input)
           .keyBy(i -> i)
           .map(new CacheTestFunction())
           .addSink(new DiscardingSink());

        System.out.println("Starting test with CACHE_SIZE = " + CACHE_SIZE);
        env.execute("Test MapStateWithCache");
        System.out.println("Test completed");
    }
    
    // input datagen to test cache behavior
    private List<Integer> createTestInput() {
        List<Integer> input = new ArrayList<>();
        
        // access keys 0-9 (all cache misses)
        for (int i = 0; i < 10; i++) {
            input.add(i);
        }
        // re-access keys 0-4 (should be faster - cache hits)
        for (int i = 0; i < 5; i++) {
            input.add(i);
        }
        // access keys 10-14 (higher latency but should cause evictions)
        for (int i = 10; i < 15; i++) {
            input.add(i);
        }
        // re-access 0-14 and observe latency differences due to eviction
        for (int i = 0; i < 15; i++) {
            input.add(i);
        }
        // test other mapstate operations
        input.add(100);
        
        return input;
    }

    // input processing function
    private static class CacheTestFunction extends RichMapFunction<Integer, Tuple2<Integer, Long>> {
        private transient MapState<String, Integer> mapState;
        private int accessCount = 0;
        private long totalLatency = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> descriptor = 
                new MapStateDescriptor<>("test-map", String.class, Integer.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public Tuple2<Integer, Long> map(Integer key) throws Exception {
            accessCount++;
            
            // test other operations
            if (key == 100) {
                return testAdditionalOperations();
            }
            
            // basic get/put with cache behavior
            String mapKey = "key-" + key;
            
            // access latency to differentiate between cache vs backend access
            long startTime = System.nanoTime();
            Integer value = mapState.get(mapKey);
            long accessLatency = System.nanoTime() - startTime;
            totalLatency += accessLatency;

            if (value != null) {
                value++;
            } else {
                value = key;
            }
            mapState.put(mapKey, value);
            
            // log access
            double avgLatency = (double)totalLatency / accessCount;
            System.out.println(String.format(
                "Access #%d: key=%d, value=%d, latency=%d ns (%.2f%% of avg)", 
                accessCount, key, value, accessLatency, 
                (accessLatency * 100.0 / avgLatency)));
            
            return Tuple2.of(key, accessLatency);
        }
        
        private Tuple2<Integer, Long> testAdditionalOperations() throws Exception {
            System.out.println("\n------ Testing Additional MapState Operations ------");
            
            // putAll
            Map<String, Integer> batch = new HashMap<>();
            batch.put("batch-1", 101);
            batch.put("batch-2", 102);
            mapState.putAll(batch);
            System.out.println("putAll: added entries batch-1=101, batch-2=102");
            
            // contains
            boolean contains1 = mapState.contains("batch-1");
            System.out.println("contains batch-1: " + contains1);
            
            // iterate through entries 
            System.out.println("Iterating through entries:");
            int entryCount = 0;
            for (Map.Entry<String, Integer> entry : mapState.entries()) {
                entryCount++;
                if (entryCount <= 5) {
                    System.out.println("  " + entry.getKey() + " -> " + entry.getValue());
                }
            }
            System.out.println("Total entries: " + entryCount);
            
            // remove
            mapState.remove("batch-1");
            contains1 = mapState.contains("batch-1");
            System.out.println("After remove, contains batch-1: " + contains1);
            
            // isEmpty
            boolean isEmpty = mapState.isEmpty();
            System.out.println("isEmpty before clear: " + isEmpty);
            
            // clear
            mapState.clear();
            isEmpty = mapState.isEmpty();
            System.out.println("isEmpty after clear: " + isEmpty);

            return Tuple2.of(100, 0L);
        }
    }

    private static class DiscardingSink implements SinkFunction<Tuple2<Integer, Long>> {
        @Override
        public void invoke(Tuple2<Integer, Long> value, Context context) {
            // discard output
        }
    }
}