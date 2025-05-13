package org.apache.flink.test.nexmark;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;

/**
 * Benchmark for Nexmark Q3-style join operations with controlled delays and cache sizes.
 * Measures how the cache size affects performance when processing streams with out-of-order events.
 */
public class JoinStatePerformanceTest extends TestLogger {

    @TempDir
    Path tmp;
    
    // --- Benchmark parameters ---
    private static final int NUM_PEOPLE = 500_000;
    private static final int NUM_AUCTIONS = 500_000;
    
    // Watermark configuration
    private static final long JOIN_WINDOW_SIZE_MS = 30000;
    private static final long WATERMARK_LATENESS_MS = 30200;
    
    // Delay parameters
    private static final long MIN_DELAY_MS = 1050;
    private static final long MAX_DELAY_MS = 26000;
    
    // Different delay probabilities to test
    private static final double[] DELAY_PROBABILITIES = {0.0, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3};
    // private static final double[] DELAY_PROBABILITIES = {0.0, 0.02, 0.05, 0.1};
    // private static final double[] DELAY_PROBABILITIES = {0.0};
    
    // Different cache sizes to test (0 means native RocksDB without cache layer)
    // private static final int[] CACHE_SIZES = {1, 5, 10, 50, 100, 200, 500, 1000, 1500, 2000, 3000, 10000};
    private static final int[] CACHE_SIZES = {5, 50, 100, 500, 1000};
    // private static final int[] CACHE_SIZES = {1};
    
    // Stack distance histogram settings
    private static final int STACK_DISTANCE_BUCKETS = 18;
    
    // Time histogram settings (in microseconds)
    private static final int NUM_TIME_HISTOGRAM_BUCKETS = 16;
    private static final double[] TIME_BUCKET_BOUNDARIES = new double[NUM_TIME_HISTOGRAM_BUCKETS];
    
    static {
        // Initialize histogram buckets (exponential: 1µs, 2µs, 4µs, 8µs, ...)
        TIME_BUCKET_BOUNDARIES[0] = 0.0078125; // Start at 1 microsecond
        for (int i = 1; i < NUM_TIME_HISTOGRAM_BUCKETS; i++) {
            TIME_BUCKET_BOUNDARIES[i] = TIME_BUCKET_BOUNDARIES[i-1] * 2;
        }
    }
    
    private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility
    
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
     * Person event representing a person in Nexmark.
     */
    public static class Person implements Serializable {
        public long personId;        // Person ID
        public String name;          // Person name
        public String state;         // State
        public long timestamp;       // Event timestamp
        
        public Person() {}
        
        public Person(long personId, String name, String state, long timestamp) {
            this.personId = personId;
            this.name = name;
            this.state = state;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return "Person{id=" + personId + ", ts=" + timestamp + "}";
        }
    }
    
    /**
     * Auction event representing an auction in Nexmark.
     */
    public static class Auction implements Serializable {
        public long auctionId;       // Auction ID
        public long sellerId;        // Seller ID (references personId)
        public long initialBid;      // Initial bid price
        public long reserve;         // Reserve price
        public long timestamp;       // Event timestamp
        
        public Auction() {}
        
        public Auction(long auctionId, long sellerId, long initialBid, long reserve, long timestamp) {
            this.auctionId = auctionId;
            this.sellerId = sellerId;
            this.initialBid = initialBid;
            this.reserve = reserve;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return "Auction{id=" + auctionId + ", sellerId=" + sellerId + ", ts=" + timestamp + "}";
        }
    }
    
    /**
     * Result of joining Person and Auction.
     */
    public static class JoinResult implements Serializable {
        public long personId;
        public long auctionId;
        
        public JoinResult() {}
        
        public JoinResult(long personId, long auctionId) {
            this.personId = personId;
            this.auctionId = auctionId;
        }
        
        @Override
        public String toString() {
            return "JoinResult{personId=" + personId + ", auctionId=" + auctionId + "}";
        }
    }
    
    /**
     * Source that emits pre-generated persons.
     */
    public static class PersonSource implements SourceFunction<Person> {
        private final List<Person> people;
        private volatile boolean isRunning = true;
        
        public PersonSource(List<Person> people) {
            this.people = people;
        }
        
        @Override
        public void run(SourceContext<Person> ctx) throws Exception {
            for (Person person : people) {
                if (!isRunning) break;
                ctx.collectWithTimestamp(person, person.timestamp);
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    
    /**
     * Source that emits pre-generated auctions.
     */
    public static class AuctionSource implements SourceFunction<Auction> {
        private final List<Auction> auctions;
        private volatile boolean isRunning = true;
        
        public AuctionSource(List<Auction> auctions) {
            this.auctions = auctions;
        }
        
        @Override
        public void run(SourceContext<Auction> ctx) throws Exception {
            for (Auction auction : auctions) {
                if (!isRunning) break;
                ctx.collectWithTimestamp(auction, auction.timestamp);
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    
    /**
     * Instrumented join function that measures operation timing.
     */
    public static class InstrumentedJoinFunction extends ProcessJoinFunction<Person, Auction, JoinResult> {
        private final Map<String, LongCounter> timeHistogram = new HashMap<>();
        private final LongCounter totalTime = new LongCounter();
        private final LongCounter numJoins = new LongCounter();
        
        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator("totalTime", totalTime);
            getRuntimeContext().addAccumulator("numJoins", numJoins);
            
            // Initialize time histogram buckets
            for (int i = 0; i < NUM_TIME_HISTOGRAM_BUCKETS; i++) {
                LongCounter counter = new LongCounter();
                String bucketName = "timeBucket_" + TIME_BUCKET_BOUNDARIES[i];
                timeHistogram.put(bucketName, counter);
                getRuntimeContext().addAccumulator(bucketName, counter);
            }
        }
        
        @Override
        public void processElement(Person person, Auction auction, Context ctx, Collector<JoinResult> out) throws Exception {
            numJoins.add(1);
            
            // Measure join processing time
            long startTime = System.nanoTime();
            
            // Simplified Nexmark Q3 processing - minimal filtering
            JoinResult result = new JoinResult(person.personId, auction.auctionId);
            out.collect(result);
            
            // Record timing
            long processingTime = System.nanoTime() - startTime;
            totalTime.add(processingTime);
            
            // Add to time histogram (convert ns to µs)
            double durationMicros = processingTime / 1000.0;
            int bucketIndex = findTimeBucket(durationMicros);
            String bucketName = "timeBucket_" + TIME_BUCKET_BOUNDARIES[bucketIndex];
            timeHistogram.get(bucketName).add(1);
        }
        
        private int findTimeBucket(double durationMicros) {
            for (int i = 0; i < NUM_TIME_HISTOGRAM_BUCKETS; i++) {
                if (durationMicros < TIME_BUCKET_BOUNDARIES[i]) {
                    return i;
                }
            }
            return NUM_TIME_HISTOGRAM_BUCKETS - 1; // Last bucket for large values
        }
    }
    
    /**
     * Generate people and auctions with controlled temporal locality and delays.
     * This improved implementation uses fewer unique keys to ensure temporal locality.
     */
    private Tuple2<List<Person>, List<Auction>> generateData(double delayProbability) {
        List<Person> people = new ArrayList<>(NUM_PEOPLE);
        List<Auction> auctions = new ArrayList<>(NUM_AUCTIONS);
        
        // Current timestamp for event generation (starting at 0)
        long currentTimestamp = 0;
        
        // Create clusters of related events for good locality
        int clusterSize = 100; // Size of a cluster with similar keys
        int numClusters = NUM_PEOPLE / clusterSize;
        
        // Use fewer unique keys to ensure repeated access patterns
        int numUniqueKeys = 10_000; // 10% of total records
        
        // Generate Person data
        for (int cluster = 0; cluster < numClusters; cluster++) {
            // For each cluster, use a subset of keys
            int keysPerCluster = numUniqueKeys / numClusters;
            long baseKey = cluster * keysPerCluster;
            
            for (int i = 0; i < clusterSize; i++) {
                // Cycle through keys within the cluster to ensure repeats
                long personId = baseKey + (i % keysPerCluster);
                long eventTime = currentTimestamp;
                
                // Apply delay with probability p
                if (RANDOM.nextDouble() < delayProbability) {
                    long delay = MIN_DELAY_MS + RANDOM.nextInt((int)(MAX_DELAY_MS - MIN_DELAY_MS + 1));
                    eventTime -= delay;
                }
                
                // Create person - 20% from Oregon to make the filter pass
                Person person = new Person(
                        personId,
                        "Person" + personId,
                        RANDOM.nextDouble() < 0.2 ? "OR" : "WA",
                        eventTime
                );
                people.add(person);
                
                // Increment timestamp slightly for next event
                currentTimestamp += 1 + RANDOM.nextInt(3);
            }
            
            // Add a small gap between clusters
            currentTimestamp += 50 + RANDOM.nextInt(100);
        }
        
        // Reset timestamp for auction generation but keep the same pattern
        currentTimestamp = 0;
        
        // Generate Auction data with matching seller IDs for joins
        for (int cluster = 0; cluster < numClusters; cluster++) {
            int keysPerCluster = numUniqueKeys / numClusters;
            long baseKey = cluster * keysPerCluster;
            
            for (int i = 0; i < clusterSize; i++) {
                // Use the same ID pattern as Person for sellerId to ensure joins
                long sellerId = baseKey + (i % keysPerCluster);
                long auctionId = cluster * clusterSize + i; // Unique auction IDs
                long eventTime = currentTimestamp;
                
                // Apply delay with probability p
                if (RANDOM.nextDouble() < delayProbability) {
                    long delay = MIN_DELAY_MS + RANDOM.nextInt((int)(MAX_DELAY_MS - MIN_DELAY_MS + 1));
                    eventTime -= delay;
                }
                
                // Create auction - 50% below reserve to pass filter
                Auction auction = new Auction(
                        auctionId,
                        sellerId,
                        RANDOM.nextInt(10000),
                        RANDOM.nextInt(20000),
                        eventTime
                );
                auctions.add(auction);
                
                // Increment timestamp slightly
                currentTimestamp += 1 + RANDOM.nextInt(3);
            }
            
            // Add a small gap between clusters
            currentTimestamp += 50 + RANDOM.nextInt(100);
        }
        
        return Tuple2.of(people, auctions);
    }
    
    /**
     * Compute stack distance histogram for the given data.
     * This is an offline analysis to help understand access patterns.
     */
    private static <T> int[] computeStackDistanceHistogram(List<T> data, KeyExtractor<T> keyExtractor) {
        // First sort data by timestamp to ensure chronological ordering
        List<T> sortedData = new ArrayList<>(data);
        
        if (data.size() > 0) {
            if (data.get(0) instanceof Person) {
                sortedData.sort(Comparator.comparingLong(item -> ((Person)item).timestamp));
            } else if (data.get(0) instanceof Auction) {
                sortedData.sort(Comparator.comparingLong(item -> ((Auction)item).timestamp));
            }
        }
        
        int[] histogram = new int[STACK_DISTANCE_BUCKETS];
        Map<Long, Integer> lastSeenPosition = new HashMap<>();
        
        for (int i = 0; i < sortedData.size(); i++) {
            Long key = keyExtractor.getKey(sortedData.get(i));
            Integer lastPos = lastSeenPosition.get(key);
            
            if (lastPos != null) {
                int distance = i - lastPos - 1;
                int bucket = stackDistanceBucket(distance);
                histogram[bucket]++;
            }
            
            lastSeenPosition.put(key, i);
        }
        
        return histogram;
    }
    
    /**
     * Determine which bucket a stack distance belongs to.
     * We use power-of-2 buckets: [0], [1], [2-3], [4-7], [8-15], etc.
     */
    private static int stackDistanceBucket(int distance) {
        if (distance <= 0) return 0;
        
        // Find the highest bit position (log2)
        return 32 - Integer.numberOfLeadingZeros(distance);
    }
    
    /**
     * Interface to extract keys from different data types.
     */
    private interface KeyExtractor<T> {
        Long getKey(T item);
    }

    
    /**
     * Run a Nexmark Q3 benchmark with the given cache size and delay probability.
     */
    private void runBenchmarkWithData(int cacheSize, double delayProbability, 
                                    List<Person> people, List<Auction> auctions) throws Exception {
        System.out.println("\n===== RUNNING NEXMARK Q3 JOIN BENCHMARK =====");
        System.out.println("Cache Size: " + (cacheSize > 0 ? cacheSize : "Native RocksDB (no cache)"));
        System.out.println("Delay Probability: " + delayProbability);
        
        // Configure Flink job
        Configuration localConfig = new Configuration(config);
        if (cacheSize > 0) {
            localConfig.set(STATE_BACKEND_CACHE_SIZE, cacheSize);
        } else {
            // For native RocksDB, we don't set the cache size
            localConfig.removeConfig(STATE_BACKEND_CACHE_SIZE);
        }
        
        // Setup execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(localConfig);
        
        // Define watermark strategies
        WatermarkStrategy<Person> personWatermarkStrategy = WatermarkStrategy
                .<Person>forBoundedOutOfOrderness(Duration.ofMillis(WATERMARK_LATENESS_MS))
                .withTimestampAssigner((person, ts) -> person.timestamp);
        
        WatermarkStrategy<Auction> auctionWatermarkStrategy = WatermarkStrategy
                .<Auction>forBoundedOutOfOrderness(Duration.ofMillis(WATERMARK_LATENESS_MS))
                .withTimestampAssigner((auction, ts) -> auction.timestamp);
        
        // Create sources using the pre-generated data
        DataStream<Person> personStream = env
                .addSource(new PersonSource(people))
                .assignTimestampsAndWatermarks(personWatermarkStrategy);
        
        DataStream<Auction> auctionStream = env
                .addSource(new AuctionSource(auctions))
                .assignTimestampsAndWatermarks(auctionWatermarkStrategy);
        
        // Setup the interval join
        DataStream<JoinResult> joinedStream = personStream
                .keyBy(person -> person.personId)
                .intervalJoin(auctionStream.keyBy(auction -> auction.sellerId))
                .between(Time.milliseconds(0), Time.milliseconds(JOIN_WINDOW_SIZE_MS))
                .process(new InstrumentedJoinFunction());
        
        // Add sink
        joinedStream.addSink(new DiscardingSink<>());
        
        // Execute the job and measure total runtime
        String jobName = "NexmarkQ3_Cache" + cacheSize + "_Delay" + delayProbability;
        long startTime = System.currentTimeMillis();
        JobExecutionResult result = env.execute(jobName);
        long endTime = System.currentTimeMillis();
        long totalRuntimeMs = endTime - startTime;
        
        // Get operation metrics
        long numJoins = result.getAccumulatorResult("numJoins");
        long totalNanoTime = result.getAccumulatorResult("totalTime");
        double avgProcessingTimeNs = (double) totalNanoTime / numJoins;
        double avgProcessingTimeUs = avgProcessingTimeNs / 1000.0;
        
        // Report results
        System.out.println("\n=== Results ===");
        System.out.println("Total job runtime: " + totalRuntimeMs + " ms");
        System.out.println("Total joins performed: " + numJoins);
        System.out.println("Average join processing time: " + avgProcessingTimeUs + " μs");
        
        // Report operation time histogram
        System.out.println("\nJoin operation time histogram (microseconds):");
        System.out.println("Bucket,Count");
        for (int i = 0; i < NUM_TIME_HISTOGRAM_BUCKETS; i++) {
            String bucketName = "timeBucket_" + TIME_BUCKET_BOUNDARIES[i];
            long count = result.getAccumulatorResult(bucketName);
            System.out.println(TIME_BUCKET_BOUNDARIES[i] + "," + count);
        }
    }
    
    /**
     * Utility method to print histogram data.
     */
    private void printHistogram(int[] histogram) {
        System.out.println("Bucket,Count,UpperBound");
        
        long sum = 0;
        for (int i = 0; i < histogram.length; i++) {
            sum += histogram[i];
            String upperBound = (i == 0) ? "0" : (i == 1) ? "1" : String.valueOf((1 << (i-1)));
            System.out.println(i + "," + histogram[i] + "," + upperBound);
        }
        System.out.println("Total: " + sum);
    }
    
    @Test
    public void testNexmarkQ3WithDifferentCacheSizesAndDelays() throws Exception {
        RANDOM.setSeed(42);
        for (double delayProb : DELAY_PROBABILITIES) {
            // Generate data once per delay probability
            System.out.println("\n==== GENERATING DATA WITH DELAY PROBABILITY: " + delayProb + " ====");
            Tuple2<List<Person>, List<Auction>> data = generateData(delayProb);
            List<Person> people = data.f0;
            List<Auction> auctions = data.f1;
            
            // Compute stack distances as offline analysis (once per dataset)
            System.out.println("\nComputing stack distance histograms...");
            int[] personHistogram = computeStackDistanceHistogram(people, 
                    (KeyExtractor<Person>) person -> person.personId);
            int[] auctionHistogram = computeStackDistanceHistogram(auctions, 
                    (KeyExtractor<Auction>) auction -> auction.sellerId);
            
            // Print stack distance histograms
            System.out.println("\nStack Distance Histogram for Person keys:");
            printHistogram(personHistogram);
            System.out.println("\nStack Distance Histogram for Auction keys (sellerId):");
            printHistogram(auctionHistogram);
            
            // Now test all cache sizes with the same data
            for (int cacheSize : CACHE_SIZES) {
                runBenchmarkWithData(cacheSize, delayProb, people, auctions);
            }
        }
    }
}