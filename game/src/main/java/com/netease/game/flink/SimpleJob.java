/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.game.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SimpleJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        nums = nums.flatMap(new Neighborhood());
        nums.print();

        env.execute("Flink Job Test");
    }

    public static class Neighborhood implements FlatMapFunction<Integer, Integer> {

        @Override
        public void flatMap(Integer integer, Collector<Integer> collector) throws Exception {
            collector.collect(integer - 2);
            collector.collect(integer + 2);
        }
    }

    @Test
    public void testNeighborhood() throws Exception {
        Neighborhood neighborhood = new Neighborhood();
        List<Integer> result = new ArrayList<>();
        Collector<Integer> collector = new ListCollector<>(result);
        neighborhood.flatMap(3, collector);
        System.out.println(result);
    }

}


