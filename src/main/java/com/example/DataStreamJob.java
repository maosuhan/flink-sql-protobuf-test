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

package com.example;

import org.apache.flink.formats.protobuf.testproto.MapTest;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        TestProtobufTestStore.sourcePbInputs.clear();
        TestProtobufTestStore.sourcePbInputs.add(MapTest.getDefaultInstance().toByteArray());
        TestProtobufTestStore.sinkResults.clear();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inBatchMode().build());
        String sql =
                "create table bigdata_sink ( "
                        + "	a int, "
                        + "	map1 map<string,string>,"
                        + " map2 map<string, row<a int, b bigint>>"
                        + ")  with ("
                        + "	'connector' = 'protobuf-test-connector', "
                        + "	'format' = 'protobuf', "
                        + " 'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.MapTest', "
                        + " 'protobuf.write-null-string-literal' = 'NULL' "
                        + ")";
        tEnv.executeSql(sql);

        TableResult tableResult = tEnv.executeSql(
                "insert into bigdata_sink select 2, map['a', null], map['b', cast(null as row<a int, b bigint>)]");
        tableResult.await();
        byte[] bytes = TestProtobufTestStore.sinkResults.get(0);
        MapTest mapTest = MapTest.parseFrom(bytes);
        System.out.println(mapTest.getA());
    }
}
