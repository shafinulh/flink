package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

public class MultiStreamStateBenchmarkTest extends TestLogger {
    @TempDir Path tmp;

    private static final long NUM_ORDERS = 1000000;
    private static final long NUM_UPDATES = 200000;
    private static final int CACHE_SIZE = 10000;
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(1000);

    private Configuration config;

    @BeforeEach
    public void before() throws IOException {
        String checkpointDir = "file://" + Files.createTempDirectory(tmp, "test").toString();
        config = new Configuration();
        config.set(STATE_BACKEND, "rocksdb");
        config.set(CHECKPOINTING_INTERVAL, CHECKPOINT_INTERVAL);
    }

    @Test
    public void testMultiStreamWithCache() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_SIZE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        System.out.println("[Benchmark] Running with RocksDB + Cache (Cache Size = " + CACHE_SIZE + ")");
        runBenchmark(env, "WithCache");
    }

    @Test
    public void testMultiStreamWithoutCache() throws Exception {
        config.removeConfig(STATE_BACKEND_CACHE_SIZE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        System.out.println("[Benchmark] Running with RocksDB (No Cache)");
        runBenchmark(env, "WithoutCache");
    }

    private void runBenchmark(StreamExecutionEnvironment env, String testName) throws Exception {
        DataStream<Tuple3<Long, Long, Double>> orders = env
                .fromSequence(1, NUM_ORDERS)
                .map(orderId -> new Tuple3<>(orderId, orderId % 500, (orderId % 100) * 1.5))
                .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.DOUBLE))
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        DataStream<Tuple3<Long, Long, String>> userUpdates = env
                .fromSequence(1, NUM_UPDATES)
                .map(userId -> new Tuple3<>(userId, userId % 500, (userId % 2 == 0) ? "Premium" : "Regular"))
                .returns(Types.TUPLE(Types.LONG, Types.LONG, Types.STRING))
                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        DataStream<Tuple3<Long, Double, Double>> result = orders
                .keyBy(order -> order.f1)
                .connect(userUpdates.keyBy(update -> update.f1))
                .process(new MultiStreamStateOperator());

        result.addSink(new DiscardingSink<>());

        JobExecutionResult executionResult = env.execute("MultiStreamState Benchmark - " + testName);
        System.out.println("[Benchmark Result] " + testName + " Execution Time: " + executionResult.getNetRuntime() + " ms");
    }

    private static class MultiStreamStateOperator
            extends KeyedCoProcessFunction<Long, Tuple3<Long, Long, Double>, Tuple3<Long, Long, String>, Tuple3<Long, Double, Double>> {

        private transient ValueState<String> userStatus;
        private transient ListState<Double> orderPrices;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> statusDescriptor = new ValueStateDescriptor<>("userStatus", String.class);
            userStatus = getRuntimeContext().getState(statusDescriptor);

            ListStateDescriptor<Double> orderDescriptor = new ListStateDescriptor<>("orderPrices", Double.class);
            orderPrices = getRuntimeContext().getListState(orderDescriptor);

            // System.out.println("[Operator] Initialized state descriptors");
        }

        @Override
        public void processElement1(Tuple3<Long, Long, Double> order, Context ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            orderPrices.add(order.f2);
            double total = 0;
            for (Double price : orderPrices.get()) {
                total += price;
            }
            String status = userStatus.value();
            double discountFactor = (status != null && status.equals("Premium")) ? 0.9 : 1.0;
            double discountedTotal = total * discountFactor;

            // System.out.println("[Operator] Processing Order: " +
            //         "UserID=" + order.f1 + ", OrderAmount=" + order.f2 + ", TotalAmount=" + total +
            //         ", DiscountFactor=" + discountFactor + ", DiscountedTotal=" + discountedTotal);

            out.collect(new Tuple3<>(order.f1, total, discountedTotal));
        }

        @Override
        public void processElement2(Tuple3<Long, Long, String> update, Context ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            userStatus.update(update.f2);
            double total = 0;
            for (Double price : orderPrices.get()) {
                total += price;
            }
            double discountFactor = update.f2.equals("Premium") ? 0.9 : 1.0;
            double discountedTotal = total * discountFactor;

            // System.out.println("[Operator] Processing User Update: " +
            //         "UserID=" + update.f1 + ", NewStatus=" + update.f2 + ", TotalAmount=" + total +
            //         ", DiscountFactor=" + discountFactor + ", DiscountedTotal=" + discountedTotal);

            out.collect(new Tuple3<>(update.f1, total, discountedTotal));
        }
    }
}
