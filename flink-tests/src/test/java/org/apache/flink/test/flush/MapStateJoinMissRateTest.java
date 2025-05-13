package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

public class MapStateJoinMissRateTest extends TestLogger {

    @TempDir
    Path tmp;

    private static final long NUM_RECORDS = (long) 1e6;
    private static final int CACHE_SIZE = 100;
    private static final Time WINDOW_SIZE = Time.milliseconds(100);

    private Configuration config;

    @BeforeEach
    public void setup() throws IOException {
        String checkpointDir = "file://" + Files.createTempDirectory(tmp, "test").toString();
        config = new Configuration();
        config.set(RESTART_STRATEGY, "none");
        config.set(CHECKPOINT_STORAGE, "filesystem");
        config.set(CHECKPOINTS_DIRECTORY, checkpointDir);
        config.set(OBJECT_REUSE, true);
        config.set(DEFAULT_PARALLELISM, 1);
        config.set(STATE_BACKEND, "rocksdb");
        config.set(INCREMENTAL_CHECKPOINTS, true);
    }

    @Test
    public void testRocksDB_HighHitRate() throws Exception {
        runBenchmark("rocksdb", true, 1, "RocksDB_HighHitRate");
    }

    @Test
    public void testRocksDB_MediumHitRate() throws Exception {
        runBenchmark("rocksdb", true, 50, "RocksDB_MediumHitRate");
    }

    @Test
    public void testRocksDB_LowHitRate() throws Exception {
        runBenchmark("rocksdb", true, 90, "RocksDB_LowHitRate");
    }

    @Test
    public void testRocksDBNoCache_HighHitRate() throws Exception {
        runBenchmark("rocksdb", false, 10, "RocksDBNoCache_HighHitRate");
    }

    @Test
    public void testRocksDBNoCache_MediumHitRate() throws Exception {
        runBenchmark("rocksdb", false, 50, "RocksDBNoCache_MediumHitRate");
    }

    @Test
    public void testRocksDBNoCache_LowHitRate() throws Exception {
        runBenchmark("rocksdb", false, 90, "RocksDBNoCache_LowHitRate");
    }

    @Test
    public void testHashMap_HighHitRate() throws Exception {
        runBenchmark("hashmap", false, 10, "HashMap_HighHitRate");
    }

    @Test
    public void testHashMap_MediumHitRate() throws Exception {
        runBenchmark("hashmap", false, 50, "HashMap_MediumHitRate");
    }

    @Test
    public void testHashMap_LowHitRate() throws Exception {
        runBenchmark("hashmap", false, 90, "HashMap_LowHitRate");
    }

    /**
     * Runs the join benchmark with specified backend and miss rate
     * 
     * @param backend The state backend to use ("rocksdb" or "hashmap")
     * @param useCache Whether to enable the state backend cache
     * @param missRatePercent Target miss rate (0-100)
     * @param testName Name of the test for reporting
     */
    private void runBenchmark(String backend, boolean useCache, int missRatePercent, String testName) throws Exception {
        System.out.println("Running: " + testName + " with target miss rate: " + missRatePercent + "%");
        
        // Set the backend configuration
        config.set(STATE_BACKEND, backend);
        if (useCache) {
            config.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);
        } else {
            config.removeConfig(STATE_BACKEND_CACHE_SIZE);
        }

        // Disable checkpointing
        Configuration localConfig = new Configuration(config);
        localConfig.removeConfig(CHECKPOINTING_INTERVAL);

        // Define key space parameters
        final int keySpace = CACHE_SIZE * 2; // Total number of distinct keys (2x cache size)
        // The skew interval determines how far apart records for the same key can be
        // A larger skew interval means more likely cache misses
        final int maxSkewInterval = calculateSkewInterval(missRatePercent, CACHE_SIZE);
        
        // Run three times and report runtime
        for (int i = 0; i < 3; i++) {
            // Create a new environment for each run with the same config
            StreamExecutionEnvironment runEnv = StreamExecutionEnvironment.getExecutionEnvironment(localConfig);
            
            // Stream A - Normal ordered sequence
            DataStream<Tuple3<Integer, Long, String>> streamA = runEnv
                    .addSource(new KeyedSequenceSource(NUM_RECORDS, keySpace, 0))
                    .name("Source-StreamA");

            // Stream B - With controlled skew to create cache misses
            DataStream<Tuple3<Integer, Long, String>> streamB = runEnv
                    .addSource(new KeyedSequenceSource(NUM_RECORDS, keySpace, maxSkewInterval, missRatePercent))
                    .name("Source-StreamB");

            // Join the streams
            DataStream<Tuple3<Integer, String, String>> joinedStream = streamA
                    .join(streamB)
                    .where(t -> t.f0)
                    .equalTo(t -> t.f0)
                    .window(TumblingProcessingTimeWindows.of(WINDOW_SIZE))
                    .apply(new JoinFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>, Tuple3<Integer, String, String>>() {
                        @Override
                        public Tuple3<Integer, String, String> join(
                                Tuple3<Integer, Long, String> first,
                                Tuple3<Integer, Long, String> second) {
                            return new Tuple3<>(first.f0, first.f2, second.f2);
                        }
                    });

            joinedStream.addSink(new DiscardingSink<>()).name("DiscardingSink");
            
            // Execute this iteration
            JobExecutionResult result = runEnv.execute(testName + "_run_" + i);
            System.out.println(testName + " run " + i + " with target miss rate " 
                + missRatePercent + "%: " + result.getNetRuntime() + "ms");
        }
    }

    /**
     * Calculate the skew interval based on target miss rate and cache size.
     * 
     * @param missRatePercent Target miss rate percentage (0-100)
     * @param cacheSize Size of the cache
     * @return The skew interval to use
     */
    private int calculateSkewInterval(int missRatePercent, int cacheSize) {
        // For 0% miss rate, no skew (records perfectly aligned)
        // For 100% miss rate, skew should be much larger than cache size
        return (int)(cacheSize * (1 + missRatePercent / 50.0));
    }

    /**
     * Source that generates keyed records with controllable skew between keys
     */
    private static class KeyedSequenceSource implements SourceFunction<Tuple3<Integer, Long, String>> {
        private final long numRecords;
        private final int keySpace;
        private final int maxSkewInterval;
        private final int missRatePercent;
        private final String streamName;
        private volatile boolean running = true;
        
        // Constructor for stream A (no skew)
        public KeyedSequenceSource(long numRecords, int keySpace, int maxSkewInterval) {
            this(numRecords, keySpace, maxSkewInterval, 0);
        }
        
        // Constructor for stream B (with configurable skew)
        public KeyedSequenceSource(long numRecords, int keySpace, int maxSkewInterval, int missRatePercent) {
            this.numRecords = numRecords;
            this.keySpace = keySpace;
            this.maxSkewInterval = maxSkewInterval;
            this.missRatePercent = missRatePercent;
            this.streamName = missRatePercent == 0 ? "StreamA" : "StreamB";
        }

        @Override
        public void run(SourceContext<Tuple3<Integer, Long, String>> ctx) throws Exception {
            Random random = new Random(42); // Fixed seed for reproducibility
            
            for (long i = 0; i < numRecords && running; i++) {
                // Base key assignment - sequential through key space
                int baseKey = (int)(i % keySpace);
                long value = i;
                int key;
                
                if (random.nextInt(100) < missRatePercent) {
                    // For records that should simulate a miss:
                    // Skew the key position by delaying it far enough that it would be
                    // beyond the cache capacity when its matching record arrives
                    int skew = random.nextInt(maxSkewInterval) + keySpace;
                    value = (i + skew) % numRecords; 
                    key = baseKey;
                } else {
                    // Normal case - keys aligned between streams
                    key = baseKey;
                }
                
                ctx.collect(new Tuple3<>(key, value, streamName + "-" + value));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}