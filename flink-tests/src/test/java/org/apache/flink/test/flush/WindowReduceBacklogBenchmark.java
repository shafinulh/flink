/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.flush;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;

/** HybridSourceBacklogExample. */
public class WindowReduceBacklogBenchmark {
    public static void main(String[] args) throws Exception {

        int numKeys = Integer.parseInt(args[0]);
        long numBacklogRecords = Long.parseLong(args[1]);
        long numRealTimeRecords = Long.parseLong(args[2]);
        boolean enableBacklog = Boolean.parseBoolean(args[3]);
        boolean useBatch = Boolean.parseBoolean(args[4]);

        final Configuration config = new Configuration();
        config.set(STATE_BACKEND, "rocksdb");
        if (useBatch) {
            config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        } else if (enableBacklog) {
//            config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
           config.set(STATE_BACKEND_CACHE_SIZE, 10);
        }

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.setParallelism(1);

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> historicalData =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple3<Integer, Long, Long>>() {
                            @Override
                            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                                return new Tuple3<>(value.intValue() % numKeys, value, 1L);
                            }
                        },
                        numBacklogRecords,
                        Types.TUPLE(Types.INT, Types.LONG));

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> realTimeData =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple3<Integer, Long, Long>>() {

                            @Override
                            public Tuple3<Integer, Long, Long> map(Long value) throws Exception {
                                return new Tuple3<>(
                                        value.intValue() % numKeys, value + numBacklogRecords, 1L);
                            }
                        },
                        numRealTimeRecords,
                        Types.TUPLE(Types.INT, Types.LONG));

        final HybridSource<Tuple3<Integer, Long, Long>> source =
                HybridSource.builder(historicalData).addSource(realTimeData).build();
        final SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> reduced =
                env.fromSource(
                                source,
                                WatermarkStrategy
                                        .<Tuple3<Integer, Long, Long>>forMonotonousTimestamps()
                                        .withTimestampAssigner((event, timestamp) -> event.f1),
                                "source")
                        .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG))
                        .keyBy(record -> record.f0)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(numKeys * 2L)))
                        .process(
                                new ProcessWindowFunction<
                                        Tuple3<Integer, Long, Long>,
                                        Tuple3<Integer, Long, Long>,
                                        Integer,
                                        TimeWindow>() {
                                    @Override
                                    public void process(
                                            Integer integer,
                                            ProcessWindowFunction<
                                                    Tuple3<Integer, Long, Long>,
                                                    Tuple3<Integer, Long, Long>,
                                                    Integer,
                                                    TimeWindow>
                                                    .Context
                                                    context,
                                            Iterable<Tuple3<Integer, Long, Long>> elements,
                                            Collector<Tuple3<Integer, Long, Long>> out)
                                            throws Exception {
                                        long sum = 0;
                                        long maxTs = Long.MIN_VALUE;
                                        for (Tuple3<Integer, Long, Long> e : elements) {
                                            sum += e.f2;
                                            maxTs = Math.max(maxTs, e.f1);
                                        }
                                        out.collect(new Tuple3<>(integer, maxTs, sum));
                                    }
                                });

        //        reduced.addSink(new DiscardingSink<>());
        reduced.print();

        long start = System.currentTimeMillis();
        env.execute("Flink Benchmark Job");
        double totalTime = (System.currentTimeMillis() - start) / 1000.0;

        System.out.println(
                "backlog count: "
                        + numBacklogRecords
                        + " realtime count: "
                        + numRealTimeRecords
                        + " time: "
                        + totalTime);
    }
}
