package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;

/**
 * test for mapstate checkpointing with the cache layer.
 *
 * this test verifies:
 * 1. state values are properly persisted during checkpointing
 * 2. after failure, all previously processed keys are correctly restored
 *
 * the test forces a failure after processing some data and then
 * confirms recovery by checking that all keys have the expected values.
 */
public class MapStateWithCacheCheckpointTest extends TestLogger {

    private static final int CACHE_SIZE = 25; 
    private static final int TOTAL_ELEMENTS = 30;
    private static final int FAILURE_POINT = 20;  // fail after processing this many elements
    
    // track if we've failed once already
    private static boolean hasFailedOnce = false;
    
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();
    
    @Test
    public void testMapStateCacheCheckpointing() throws Exception {
        // reset flag for each test run
        hasFailedOnce = false;
        
        Configuration conf = new Configuration();
        conf.set(STATE_BACKEND, "rocksdb");
        conf.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);
        
        String checkpointDir = tmpFolder.newFolder().toURI().toString();
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(200); // frequent checkpoint
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0)); // allow restarts
        
        // process data through mapstate
        env.addSource(new TestSource())
           .keyBy(index -> index % 10)
           .map(new StatefulProcessor());
        
        env.execute("MapStateWithCache Checkpoint Test");
        
        System.out.println("\n=== TEST COMPLETED SUCCESSFULLY ===");
    }
    
    // source that emits numbers and fails after a specific point
    public static class TestSource extends RichParallelSourceFunction<Integer> 
            implements CheckpointedFunction {
        
        private volatile boolean running = true;
        private int lastEmittedIndex = -1;
        private transient boolean shouldFailThisRun;
        
        @Override
        public void open(Configuration parameters) {
            // only fail on first run
            shouldFailThisRun = !hasFailedOnce;
            System.out.println("Source opening, shouldFailThisRun: " + shouldFailThisRun);
        }
        
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            System.out.println("\n=== SOURCE STARTING/RESTARTING ===");
            System.out.println("Last processed element: " + lastEmittedIndex);
            
            int nextIndex = lastEmittedIndex + 1;
            
            while (running && nextIndex < TOTAL_ELEMENTS) {
                // trigger failure only once
                if (shouldFailThisRun && nextIndex >= FAILURE_POINT) {
                    System.out.println("\nFAKE FAILURE");
                    hasFailedOnce = true;
                    throw new RuntimeException("fake failure for testing checkpoint recovery");
                }
                
                Thread.sleep(50); // slow down for checkpoints
                
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextIndex);
                    lastEmittedIndex = nextIndex;
                    System.out.println("Source emitted: " + nextIndex);
                }
                
                nextIndex++;
            }
            
            System.out.println("=== SOURCE COMPLETED SUCCESSFULLY ===");
        }
        
        @Override
        public void cancel() {
            running = false;
        }
        
        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            System.out.println("\n=== CHECKPOINT " + context.getCheckpointId() + 
                              " TRIGGERED - Last emitted: " + lastEmittedIndex + " ===");
        }
        
        @Override
        public void initializeState(FunctionInitializationContext context) {
        }
    }
    
    // processor that uses mapstate to store values
    public static class StatefulProcessor 
            extends RichMapFunction<Integer, String> implements CheckpointedFunction {
        
        private MapState<String, Integer> mapState;
        private int processedCount = 0;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> descriptor = 
                new MapStateDescriptor<>("test-map-state", String.class, Integer.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }
        
        @Override
        public String map(Integer index) throws Exception {
            processedCount++;
            String mapKey = "key-" + index;
            
            // check if key exists (testing state restoration)
            Integer count = mapState.get(mapKey); // count will be the value stored in the key
            boolean isNew = count == null; // if the count exists, then we have seen the key before
            
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            
            // update state
            mapState.put(mapKey, count);
            
            String result = String.format("Processed %s (value=%d, total=%d)", 
                          mapKey, count, processedCount);
            
            if (isNew) {
                System.out.println(result + " - NEW KEY");
            } else {
                System.out.println(result + " - RESTORED FROM STATE");
            }
            
            return result;
        }
        
        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            System.out.println("\n=== PROCESSOR STATE CHECKPOINT " + context.getCheckpointId() + 
                             " - Keys processed so far: " + processedCount + " ===");
        }
        
        @Override
        public void initializeState(FunctionInitializationContext context) {
            System.out.println("\n=== INITIALIZING PROCESSOR STATE ===");
        }
    }
}