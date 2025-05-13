package org.apache.flink.test.performance;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;

/**
 * Performance test for ValueState operations with and without caching.
 * This test follows the methodology from storage system literature:
 * - Measures read/write performance on random and skewed key distributions
 * - Tests different cache sizes (0 [native RocksDB], 5000, 10000, 20000 entries)
 * - Uses a warm-up phase to initialize the system before measurement
 * - Separately measures read and write operations
 */
public class ValueStatePerformanceTest extends TestLogger {

    @TempDir
    Path tmp;

    // Total number of unique keys in the dataset (1M as per literature)
    private static final int NUM_UNIQUE_KEYS = 1_000_000;
    
    // Total number of operations to perform for each test
    private static final long NUM_OPERATIONS = 1_000_000;
    
    // Number of operations for warm-up phase
    private static final long WARMUP_OPERATIONS = 1_000_000;
    
    // Different cache sizes to test (0 means native RocksDB without cache layer)
    private static final int[] CACHE_SIZES = {0, 5000, 10000, 20000};
    
    // Value size settings (as per literature - 320 bytes)
    private static final int VALUE_SIZE_BYTES = 320;
    
    // Key size settings (as per literature - 80 bytes)
    private static final int KEY_SIZE_BYTES = 80;
    
    private static final double ZIPF_SKEW = 1.2; // Standard zipfian skew parameter
    
    private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility
    
    // Time histogram settings (in microseconds)
    private static final int NUM_HISTOGRAM_BUCKETS = 20; // 24 buckets from 1µs to ~8s
    private static final double[] HISTOGRAM_BUCKET_BOUNDARIES = new double[NUM_HISTOGRAM_BUCKETS];
    
    static {
        // Initialize histogram buckets (exponential: 1µs, 2µs, 4µs, 8µs, ..., 8s)
        HISTOGRAM_BUCKET_BOUNDARIES[0] = 0.00390625; // 1 microsecond
        for (int i = 1; i < NUM_HISTOGRAM_BUCKETS; i++) {
            HISTOGRAM_BUCKET_BOUNDARIES[i] = HISTOGRAM_BUCKET_BOUNDARIES[i-1] * 2;
        }
    }
    
    private Configuration config;

    @BeforeEach
    public void setup() throws IOException {
        Files.createTempDirectory(tmp, "state");
        config = new Configuration();
        config.set(DEFAULT_PARALLELISM, 1);
        config.set(OBJECT_REUSE, true);
        config.set(STATE_BACKEND, "rocksdb");
    }

    /**
     * Generate a random string of specified length.
     */
    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            // Generate random printable ASCII characters
            char c = (char) (RANDOM.nextInt(95) + 32);
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Custom source to emit pre-generated keys for consistent testing across cache sizes.
     */
    public static class PreGeneratedKeySource implements SourceFunction<Tuple2<Long, String>> {
        private final long[] keys;
        private final boolean generateRandomValues;
        private volatile boolean isRunning = true;
        
        public PreGeneratedKeySource(long[] keys, boolean generateRandomValues) {
            this.keys = keys;
            this.generateRandomValues = generateRandomValues;
        }
        
        @Override
        public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
            int index = 0;
            while (isRunning && index < keys.length) {
                long key = keys[index++];
                // Generate a deterministic value based on key if not random
                String value = generateRandomString(VALUE_SIZE_BYTES);
                ctx.collect(new Tuple2<>(key, value));
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * Explicit key selector to avoid type issues.
     */
    public static class IntegerKeySelector implements KeySelector<Tuple2<Long, String>, Integer> {
        @Override
        public Integer getKey(Tuple2<Long, String> value) {
            return value.f0.intValue();
        }
    }

    /**
     * Operator that measures state read performance with histograms.
     */
    public static class ReadPerformanceOperator extends KeyedProcessFunction<Integer, Tuple2<Long, String>, Long> {
        private transient ValueState<String> state;
        private final LongCounter readTime = new LongCounter();
        private final LongCounter numOps = new LongCounter();
        
        // Histogram accumulators - one per bucket
        private final Map<String, LongCounter> histogramBuckets = new HashMap<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("valueState", String.class);
            state = getRuntimeContext().getState(descriptor);
            
            getRuntimeContext().addAccumulator("readTime", readTime);
            getRuntimeContext().addAccumulator("numOps", numOps);
            
            // Initialize histogram buckets
            for (int i = 0; i < NUM_HISTOGRAM_BUCKETS; i++) {
                LongCounter counter = new LongCounter();
                String bucketName = "bucket_" + HISTOGRAM_BUCKET_BOUNDARIES[i];
                histogramBuckets.put(bucketName, counter);
                getRuntimeContext().addAccumulator(bucketName, counter);
            }
        }

        @Override
        public void processElement(Tuple2<Long, String> value, Context ctx, Collector<Long> out) throws Exception {
            numOps.add(1L);
            
            // Measure read time
            long startRead = System.nanoTime();
            String currentValue = state.value();
            long readDuration = System.nanoTime() - startRead;
            readTime.add(readDuration);
            
            // Initialize if not exists
            if (currentValue == null) {
                state.update(value.f1);
            }
            
            // Add to histogram bucket (convert ns to µs)
            double durationMicros = readDuration / 1000.0;
            int bucketIndex = findBucket(durationMicros);
            String bucketName = "bucket_" + HISTOGRAM_BUCKET_BOUNDARIES[bucketIndex];
            histogramBuckets.get(bucketName).add(1);
            
            // Output the key for debugging/verification
            out.collect(value.f0);
        }
        
        private int findBucket(double durationMicros) {
            for (int i = 0; i < NUM_HISTOGRAM_BUCKETS; i++) {
                if (durationMicros < HISTOGRAM_BUCKET_BOUNDARIES[i]) {
                    return i;
                }
            }
            return NUM_HISTOGRAM_BUCKETS - 1; // Use last bucket for anything larger
        }
    }

    /**
     * Operator that measures state write performance with histograms.
     */
    public static class WritePerformanceOperator extends KeyedProcessFunction<Integer, Tuple2<Long, String>, Long> {
        private transient ValueState<String> state;
        private final LongCounter writeTime = new LongCounter();
        private final LongCounter numOps = new LongCounter();
        
        // Histogram accumulators - one per bucket
        private final Map<String, LongCounter> histogramBuckets = new HashMap<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("valueState", String.class);
            state = getRuntimeContext().getState(descriptor);
            
            getRuntimeContext().addAccumulator("writeTime", writeTime);
            getRuntimeContext().addAccumulator("numOps", numOps);
            
            // Initialize histogram buckets
            for (int i = 0; i < NUM_HISTOGRAM_BUCKETS; i++) {
                LongCounter counter = new LongCounter();
                String bucketName = "bucket_" + HISTOGRAM_BUCKET_BOUNDARIES[i];
                histogramBuckets.put(bucketName, counter);
                getRuntimeContext().addAccumulator(bucketName, counter);
            }
        }

        @Override
        public void processElement(Tuple2<Long, String> value, Context ctx, Collector<Long> out) throws Exception {
            numOps.add(1L);
            
            // Measure write time
            long startWrite = System.nanoTime();
            state.update(value.f1);
            long writeDuration = System.nanoTime() - startWrite;
            writeTime.add(writeDuration);
            
            // Add to histogram bucket (convert ns to µs)
            double durationMicros = writeDuration / 1000.0;
            int bucketIndex = findBucket(durationMicros);
            String bucketName = "bucket_" + HISTOGRAM_BUCKET_BOUNDARIES[bucketIndex];
            histogramBuckets.get(bucketName).add(1);
            
            // Output the key for debugging/verification
            out.collect(value.f0);
        }
        
        private int findBucket(double durationMicros) {
            for (int i = 0; i < NUM_HISTOGRAM_BUCKETS; i++) {
                if (durationMicros < HISTOGRAM_BUCKET_BOUNDARIES[i]) {
                    return i;
                }
            }
            return NUM_HISTOGRAM_BUCKETS - 1; // Use last bucket for anything larger
        }
    }

    /**
     * Operator for warm-up phase - populates state for all keys.
     */
    public static class WarmupOperator extends KeyedProcessFunction<Integer, Tuple2<Long, String>, Long> {
        private transient ValueState<String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("valueState", String.class);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<Long, String> value, Context ctx, Collector<Long> out) throws Exception {
            // Just update the state with the incoming value
            state.update(value.f1);
            out.collect(value.f0);
        }
    }

    /**
     * Generate keys with a Zipfian distribution.
     */
    private static long[] generateZipfianKeys(int numKeys, int count, double skew) {
        double[] distribution = createZipfianDistribution(numKeys, skew);
        long[] keys = new long[count];
        
        for (int i = 0; i < count; i++) {
            double rand = RANDOM.nextDouble();
            int selectedKey = findZipfianKey(distribution, rand);
            keys[i] = selectedKey;
        }
        
        return keys;
    }
    
    private static double[] createZipfianDistribution(int numKeys, double exponent) {
        double[] distribution = new double[numKeys];
        double sum = 0.0;
        
        for (int i = 0; i < numKeys; i++) {
            distribution[i] = 1.0 / Math.pow(i + 1, exponent);
            sum += distribution[i];
        }
        
        // Normalize to create cumulative probability distribution
        double cumulative = 0.0;
        for (int i = 0; i < numKeys; i++) {
            distribution[i] = distribution[i] / sum;
            cumulative += distribution[i];
            distribution[i] = cumulative;
        }
        
        return distribution;
    }
    
    private static int findZipfianKey(double[] distribution, double rand) {
        int left = 0;
        int right = distribution.length - 1;
        
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (distribution[mid] < rand) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        
        return left;
    }

    /**
     * Run a benchmark with given configuration.
     */
    private void runBenchmark(
            String benchmarkName,
            int cacheSize,
            boolean isRandomAccess,
            boolean isReadTest,
            double zipfSkew,
            long[] warmupKeys,
            long[] benchmarkKeys) throws Exception {
        
        Configuration localConfig = new Configuration(config);
        
        // Only set cache size if > 0 (0 means native RocksDB without cache layer)
        if (cacheSize > 0) {
            localConfig.set(STATE_BACKEND_CACHE_SIZE, cacheSize);
            System.out.println("Testing with cache size: " + cacheSize);
        } else {
            System.out.println("Testing with native RocksDB (no cache layer)");
        }
        
        // First run: Warm-up phase
        System.out.println("Starting warm-up phase for " + benchmarkName + (cacheSize > 0 ? " with cache size " + cacheSize : " with native RocksDB"));
        StreamExecutionEnvironment warmupEnv = StreamExecutionEnvironment.getExecutionEnvironment(localConfig);
        
        // Use the pre-generated warmup keys
        DataStream<Tuple2<Long, String>> warmupSource = warmupEnv
                .addSource(new PreGeneratedKeySource(warmupKeys, true))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}));
        
        warmupSource.keyBy(new IntegerKeySelector())
                .process(new WarmupOperator())
                .addSink(new DiscardingSink<>());
        
        warmupEnv.setRestartStrategy(RestartStrategies.noRestart());
        String warmupJobName = "Warmup_" + benchmarkName + (cacheSize > 0 ? "_Cache" + cacheSize : "_NativeRocksDB");
        warmupEnv.execute(warmupJobName);
        System.out.println("Warm-up phase completed");
        
        // Second run: Actual benchmark
        System.out.println("Starting measurement phase for " + benchmarkName + (cacheSize > 0 ? " with cache size " + cacheSize : " with native RocksDB"));
        StreamExecutionEnvironment benchEnv = StreamExecutionEnvironment.getExecutionEnvironment(localConfig);
        
        // Use the pre-generated benchmark keys
        DataStream<Tuple2<Long, String>> benchSource = benchEnv
                .addSource(new PreGeneratedKeySource(benchmarkKeys, true))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}));
        
        JobExecutionResult result;
        if (isReadTest) {
            benchSource.keyBy(new IntegerKeySelector())
                    .process(new ReadPerformanceOperator())
                    .addSink(new DiscardingSink<>());
        } else {
            benchSource.keyBy(new IntegerKeySelector())
                    .process(new WritePerformanceOperator())
                    .addSink(new DiscardingSink<>());
        }
        
        benchEnv.setRestartStrategy(RestartStrategies.noRestart());
        String benchJobName = benchmarkName + (cacheSize > 0 ? "_Cache" + cacheSize : "_NativeRocksDB");
        result = benchEnv.execute(benchJobName);
        
        // Process and report results
        long totalOps = result.getAccumulatorResult("numOps");
        String configLabel = cacheSize > 0 ? "with cache size " + cacheSize : "with native RocksDB (no cache)";
        
        if (isReadTest) {
            long totalReadTime = result.getAccumulatorResult("readTime");
            double avgReadTimeNs = (double) totalReadTime / totalOps;
            double avgReadTimeUs = avgReadTimeNs / 1000.0;
            
            System.out.println("=== " + benchmarkName + " " + configLabel + " ===");
            System.out.println("Total operations: " + totalOps);
            System.out.println("Average read time: " + avgReadTimeUs + " μs");
            
            // Report histogram for read times
            System.out.println("Read time histogram (microseconds):");
            System.out.println("Bucket,Count");
            for (int i = 0; i < NUM_HISTOGRAM_BUCKETS; i++) {
                String bucketName = "bucket_" + HISTOGRAM_BUCKET_BOUNDARIES[i];
                long count = result.getAccumulatorResult(bucketName);
                System.out.println(HISTOGRAM_BUCKET_BOUNDARIES[i] + "," + count);
            }
        } else {
            long totalWriteTime = result.getAccumulatorResult("writeTime");
            double avgWriteTimeNs = (double) totalWriteTime / totalOps;
            double avgWriteTimeUs = avgWriteTimeNs / 1000.0;
            
            System.out.println("=== " + benchmarkName + " " + configLabel + " ===");
            System.out.println("Total operations: " + totalOps);
            System.out.println("Average write time: " + avgWriteTimeUs + " μs");
            
            // Report histogram for write times
            System.out.println("Write time histogram (microseconds):");
            System.out.println("Bucket,Count");
            for (int i = 0; i < NUM_HISTOGRAM_BUCKETS; i++) {
                String bucketName = "bucket_" + HISTOGRAM_BUCKET_BOUNDARIES[i];
                long count = result.getAccumulatorResult(bucketName);
                System.out.println(HISTOGRAM_BUCKET_BOUNDARIES[i] + "," + count);
            }
        }
    }

    @Test
    public void testRandomReadPerformance() throws Exception {
        System.out.println("\n===== RANDOM READ PERFORMANCE TEST =====");
        
        // Pre-generate keys for both warmup and benchmark phases
        System.out.println("Generating random keys for consistent testing...");
        long[] warmupKeys = new long[(int) WARMUP_OPERATIONS];
        long[] benchmarkKeys = new long[(int) NUM_OPERATIONS];
        
        // Initialize both with random keys
        for (int i = 0; i < WARMUP_OPERATIONS; i++) {
            warmupKeys[i] = RANDOM.nextInt(NUM_UNIQUE_KEYS);
        }
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            benchmarkKeys[i] = RANDOM.nextInt(NUM_UNIQUE_KEYS);
        }
        
        // Run the benchmark with the same keys for all cache sizes
        for (int cacheSize : CACHE_SIZES) {
            runBenchmark("RandomRead", cacheSize, true, true, 0.0, warmupKeys, benchmarkKeys);
        }
    }

    @Test
    public void testRandomWritePerformance() throws Exception {
        System.out.println("\n===== RANDOM WRITE PERFORMANCE TEST =====");
        
        // Pre-generate keys for both warmup and benchmark phases
        System.out.println("Generating random keys for consistent testing...");
        long[] warmupKeys = new long[(int) WARMUP_OPERATIONS];
        long[] benchmarkKeys = new long[(int) NUM_OPERATIONS];
        
        // Initialize both with random keys
        for (int i = 0; i < WARMUP_OPERATIONS; i++) {
            warmupKeys[i] = RANDOM.nextInt(NUM_UNIQUE_KEYS);
        }
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            benchmarkKeys[i] = RANDOM.nextInt(NUM_UNIQUE_KEYS);
        }
        
        // Run the benchmark with the same keys for all cache sizes
        for (int cacheSize : CACHE_SIZES) {
            runBenchmark("RandomWrite", cacheSize, true, false, 0.0, warmupKeys, benchmarkKeys);
        }
    }

    @Test
    public void testZipfianReadPerformance() throws Exception {
        System.out.println("\n===== ZIPFIAN (HOT KEYS) READ PERFORMANCE TEST =====");
        
        // Pre-generate zipfian keys for both warmup and benchmark phases
        System.out.println("Generating zipfian keys (skew=" + ZIPF_SKEW + ") for consistent testing...");
        long[] warmupKeys = generateZipfianKeys(NUM_UNIQUE_KEYS, (int) WARMUP_OPERATIONS, ZIPF_SKEW);
        long[] benchmarkKeys = generateZipfianKeys(NUM_UNIQUE_KEYS, (int) NUM_OPERATIONS, ZIPF_SKEW);
        
        // Run the benchmark with the same keys for all cache sizes
        for (int cacheSize : CACHE_SIZES) {
            runBenchmark("ZipfianRead_Skew" + ZIPF_SKEW, cacheSize, false, true, ZIPF_SKEW, warmupKeys, benchmarkKeys);
        }
    }

    @Test
    public void testZipfianWritePerformance() throws Exception {
        System.out.println("\n===== ZIPFIAN (HOT KEYS) WRITE PERFORMANCE TEST =====");
        
        // Pre-generate zipfian keys for both warmup and benchmark phases
        System.out.println("Generating zipfian keys (skew=" + ZIPF_SKEW + ") for consistent testing...");
        long[] warmupKeys = generateZipfianKeys(NUM_UNIQUE_KEYS, (int) WARMUP_OPERATIONS, ZIPF_SKEW);
        long[] benchmarkKeys = generateZipfianKeys(NUM_UNIQUE_KEYS, (int) NUM_OPERATIONS, ZIPF_SKEW);
        
        // Run the benchmark with the same keys for all cache sizes
        for (int cacheSize : CACHE_SIZES) {
            runBenchmark("ZipfianWrite_Skew" + ZIPF_SKEW, cacheSize, false, false, ZIPF_SKEW, warmupKeys, benchmarkKeys);
        }
    }
}