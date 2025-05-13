package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/**
 * Benchmark test for MapStateWithCache using a stateful join implementation.
 * This test creates a join operator that explicitly uses MapState
 */
public class MapStateJoinBenchmarkTest extends TestLogger {
    @TempDir Path tmp;

    // Default settings
    private static final long NUM_RECORDS = 300000; 
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(1); // Longer checkpoint interval
    private static final int CACHE_SIZE = 1000;
    
    private Configuration config;
    
    @BeforeEach
    public void before() throws IOException {
        String checkpointDir = "file://" + Files.createTempDirectory(tmp, "test").toString();
        config = new Configuration();
        config.set(RESTART_STRATEGY, "none");
        config.set(CHECKPOINT_STORAGE, "filesystem");
        config.set(CHECKPOINTS_DIRECTORY, checkpointDir);
        config.set(OBJECT_REUSE, true);
        config.set(CHECKPOINTING_INTERVAL, CHECKPOINT_INTERVAL);
        config.set(DEFAULT_PARALLELISM, 1);
        config.set(STATE_BACKEND, "rocksdb");
        config.set(INCREMENTAL_CHECKPOINTS, true);
    }

    @Test
    public void testRocksDBJoin_LowCacheHitRate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.1);
    }
    
    @Test
    public void testRocksDBJoin_MediumCacheHitRate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.5);
    }
    
    @Test
    public void testRocksDBJoin_HighCacheHitRate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.9);
    }
    
    @Test
    public void testHashMapJoin_LowCacheHitRate() throws Exception {
        config.set(STATE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.1);
    }
    
    @Test
    public void testHashMapJoin_MediumCacheHitRate() throws Exception {
        config.set(STATE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.5);
    }
    
    @Test
    public void testHashMapJoin_HighCacheHitRate() throws Exception {
        config.set(STATE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.9);
    }
    
    @Test
    public void testRocksDBWithCacheJoin_LowCacheHitRate() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.1);
    }
    
    @Test
    public void testRocksDBWithCacheJoin_MediumCacheHitRate() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.5);
    }
    
    @Test
    public void testRocksDBWithCacheJoin_HighCacheHitRate() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env, 0.9);
    }
    
    /**
     * Run the join benchmark with specified cache hit rate.
     */
    private void test(StreamExecutionEnvironment env, double cacheHitRate) throws Exception {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        System.out.println("Running: " + methodName + " with cache hit rate: " + cacheHitRate);
        
        for (int i = 0; i < 3; i++) {
            StreamExecutionEnvironment runEnv = StreamExecutionEnvironment.getExecutionEnvironment(config);
            
            // config key space to achieve desired cache hit rate
            int totalKeySpace = CACHE_SIZE * 10;
            int hotKeySpace = (int) (CACHE_SIZE * cacheHitRate);
            
            // datagen stream A
            DataStream<Tuple2<String, Tuple3<Integer, Long, String>>> stream1 = runEnv
                    .fromSequence(0, NUM_RECORDS - 1)
                    .map(value -> {
                        int key;
                        if (Math.random() < cacheHitRate) {
                            key = (int)(Math.random() * hotKeySpace);
                        } else {
                            key = hotKeySpace + (int)(Math.random() * (totalKeySpace - hotKeySpace));
                        }
                        return new Tuple3<>(key, value, "Stream1-" + value);
                    })
                    .returns(Types.TUPLE(Types.INT, Types.LONG, Types.STRING))
                    .map(t -> new Tuple2<>("A", t))
                    .returns(Types.TUPLE(Types.STRING, Types.TUPLE(Types.INT, Types.LONG, Types.STRING)));

            // datagen stream B
            DataStream<Tuple2<String, Tuple3<Integer, Long, String>>> stream2 = runEnv
                    .fromSequence(0, NUM_RECORDS - 1)
                    .map(value -> {
                        int key;
                        if (Math.random() < cacheHitRate) {
                            key = (int)(Math.random() * hotKeySpace);
                        } else {
                            key = hotKeySpace + (int)(Math.random() * (totalKeySpace - hotKeySpace));
                        }
                        return new Tuple3<>(key, value, "Stream2-" + value);
                    })
                    .returns(Types.TUPLE(Types.INT, Types.LONG, Types.STRING))
                    .map(t -> new Tuple2<>("B", t))
                    .returns(Types.TUPLE(Types.STRING, Types.TUPLE(Types.INT, Types.LONG, Types.STRING)));

            // combine both streams to be consumed by operator
            DataStream<Tuple2<String, Tuple3<Integer, Long, String>>> combinedStream = 
                    stream1.union(stream2);

            // process with stateful join operator
            DataStream<Tuple3<Integer, String, String>> joinedStream = combinedStream
                    .keyBy(t -> t.f1.f0)  // Key by the integer key
                    .flatMap(new StatefulJoinFunction())
                    .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));

            joinedStream.addSink(new DiscardingSink<>());

            JobExecutionResult result = runEnv.execute(methodName + "_run_" + i);
            System.out.println(methodName + " run " + i + " with cache hit rate " 
                + cacheHitRate + ": " + result.getNetRuntime() + "ms");
        }
    }
    
    // Stateful function that implements a regular join using MapState.
    public static class StatefulJoinFunction 
            extends RichFlatMapFunction<Tuple2<String, Tuple3<Integer, Long, String>>, Tuple3<Integer, String, String>> {
        
        private static final long serialVersionUID = 1L;
        
        private transient MapState<Long, String> sideA;
        private transient MapState<Long, String> sideB;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, String> sideADescriptor =
                    new MapStateDescriptor<>("sideA", Types.LONG, Types.STRING);
            
            MapStateDescriptor<Long, String> sideBDescriptor =
                    new MapStateDescriptor<>("sideB", Types.LONG, Types.STRING);
            
            sideA = getRuntimeContext().getMapState(sideADescriptor);
            sideB = getRuntimeContext().getMapState(sideBDescriptor);
        }
        
        @Override
        public void flatMap(Tuple2<String, Tuple3<Integer, Long, String>> value, 
                           Collector<Tuple3<Integer, String, String>> out) throws Exception {
            
            String side = value.f0;
            Tuple3<Integer, Long, String> record = value.f1;
            int key = record.f0;
            long id = record.f1;
            String data = record.f2;
            
            if (side.equals("A")) {
                sideA.put(id, data);
                
                // join with ALL records from side B (for this key)
                for (Long bId : sideB.keys()) {
                    String bData = sideB.get(bId);
                    out.collect(new Tuple3<>(key, data, bData));
                }
            } else {
                sideB.put(id, data);
                
                // join with ALL records from side A (for this key)
                for (Long aId : sideA.keys()) {
                    String aData = sideA.get(aId);
                    out.collect(new Tuple3<>(key, aData, data));
                }
            }
        }
    }
}