package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

public class MapStateActualJoinTest extends TestLogger {

    @TempDir
    Path tmp;

    private static final long NUM_RECORDS = (long) 1e6;
    private static final int CACHE_SIZE = 1000;
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

    // RocksDB backend tests
    @Test
    public void testRocksDB_LowLocality() throws Exception {
        runBenchmark(1, "rocksdb", false, "RocksDB_LowLocality");
    }

    @Test
    public void testRocksDB_MediumLocality() throws Exception {
        runBenchmark(2, "rocksdb", false, "RocksDB_MediumLocality");
    }

    @Test
    public void testRocksDB_HighLocality() throws Exception {
        runBenchmark(100, "rocksdb", false, "RocksDB_HighLocality");
    }

    // HashMap backend tests
    @Test
    public void testHashMap_LowLocality() throws Exception {
        runBenchmark(1, "hashmap", false, "HashMap_LowLocality");
    }

    @Test
    public void testHashMap_MediumLocality() throws Exception {
        runBenchmark(2, "hashmap", false, "HashMap_MediumLocality");
    }

    @Test
    public void testHashMap_HighLocality() throws Exception {
        runBenchmark(100, "hashmap", false, "HashMap_HighLocality");
    }

    // RocksDB with cache enabled tests
    @Test
    public void testRocksDBWithCache_LowLocality() throws Exception {
        runBenchmark(1, "rocksdb", true, "RocksDBWithCache_LowLocality");
    }

    @Test
    public void testRocksDBWithCache_MediumLocality() throws Exception {
        runBenchmark(2, "rocksdb", true, "RocksDBWithCache_MediumLocality");
    }

    @Test
    public void testRocksDBWithCache_HighLocality() throws Exception {
        runBenchmark(100, "rocksdb", true, "RocksDBWithCache_HighLocality");
    }

    // Run the join benchmark with specified locality parameters
    private void runBenchmark(int recordsPerKey, String backend, boolean useCache, String testName) throws Exception {
        System.out.println("Running: " + testName + " with records per key: " + recordsPerKey);
        
        // Set the backend
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
        final int mod = 2 * CACHE_SIZE;
        
        // Run three times and report runtime
        for (int i = 0; i < 3; i++) {
            // Create a new environment for each run with the same config
            StreamExecutionEnvironment runEnv = StreamExecutionEnvironment.getExecutionEnvironment(localConfig);
            
            // datagen stream A
            DataStream<Tuple3<Integer, Long, String>> streamA = runEnv
                    .fromSequence(0, NUM_RECORDS - 1)
                    .map(value -> {
                        int key = (int)((value / recordsPerKey) % mod);
                        return new Tuple3<>(key, value, "StreamA-" + value);
                    })
                    .returns(Types.TUPLE(Types.INT, Types.LONG, Types.STRING));

            // datagen stream B
            DataStream<Tuple3<Integer, Long, String>> streamB = runEnv
                    .fromSequence(0, NUM_RECORDS - 1)
                    .map(value -> {
                        int key = (int)((value / recordsPerKey) % mod);
                        return new Tuple3<>(key, value, "StreamB-" + value);
                    })
                    .returns(Types.TUPLE(Types.INT, Types.LONG, Types.STRING));

            // join the streams
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

            joinedStream.addSink(new DiscardingSink<>());
            
            // to ensure termination
            runEnv.setRestartStrategy(RestartStrategies.noRestart());

            // execute this iteration
            JobExecutionResult result = runEnv.execute(testName + "_run_" + i);
            System.out.println(testName + " run " + i + " with records per key " 
                + recordsPerKey + ": " + result.getNetRuntime() + "ms");
        }
    }
}