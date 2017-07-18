/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.batch.source.BatchInputFormat;
import org.apache.flink.examples.java.batch.source.GenerateBatch;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.examples.java.batch.Utils.BATCH_SIZE;
import static org.apache.flink.examples.java.batch.Utils.DATA_SIZE;

public class FilterMapBatchRowExample {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		GenerateBatch.GenerateRowBatch generateRowBatch = new GenerateBatch.GenerateRowBatch(BATCH_SIZE);
		BatchInputFormat<List<Tuple3<Long, Long, Long>>> batchInputFormat = new BatchInputFormat(
			generateRowBatch,
			DATA_SIZE);

		TupleTypeInfo<Tuple3<Long, Long, Long>> types = TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(
			Long.class,
			Long.class,
			Long.class);

		DataSet<List<Tuple3<Long, Long, Long>>> source = new DataSource<>(env,
		                                                                  batchInputFormat,
		                                                                  new ListTypeInfo(types),
		                                                                  "row-source");

		//filter
		DataSet<List<Tuple3<Long, Long, Long>>> filter = source.flatMap(new FlatMapFunction<List<Tuple3<Long,
			Long, Long>>, List<Tuple3<Long,
			Long, Long>>>() {
			@Override
			public void flatMap(
				List<Tuple3<Long, Long, Long>> value, Collector<List<Tuple3<Long, Long, Long>>> out) throws Exception {
				List<Tuple3<Long, Long, Long>> ret = new ArrayList<Tuple3<Long, Long, Long>>();
				for (Tuple3<Long, Long, Long> tuple3 : value) {
					if (tuple3.f0 > 900 & tuple3.f1 > 900 & tuple3.f2 > 900) {
						ret.add(tuple3);
					}

				}
				if (ret.size() > 0) {
					out.collect(ret);
				}
			}
		});

		//modify-1 "col0 +1" and "col2 - 1"
		DataSet<List<Tuple3<Long, Long, Long>>> modify1 = filter.flatMap(new FlatMapFunction<List<Tuple3<Long,
			Long, Long>>, List<Tuple3<Long,
			Long, Long>>>() {
			@Override
			public void flatMap(
				List<Tuple3<Long, Long, Long>> value, Collector<List<Tuple3<Long, Long, Long>>> out) throws Exception {
				List<Tuple3<Long, Long, Long>> ret = new ArrayList<Tuple3<Long, Long, Long>>();
				for (Tuple3<Long, Long, Long> tuple3 : value) {
					ret.add(new Tuple3<Long, Long, Long>(tuple3.f0 + 1, tuple3.f1, tuple3.f2 - 1));

				}
				out.collect(ret);
			}
		});

		//modify-2 "select col2"
		DataSet<Long> modify2 = modify1.flatMap(new FlatMapFunction<List<Tuple3<Long,
			Long, Long>>, Long>() {
			@Override
			public void flatMap(
				List<Tuple3<Long, Long, Long>> value, Collector<Long> out) throws Exception {
				long batchSum = 0;
				for (Tuple3<Long, Long, Long> tuple3 : value) {
					batchSum += tuple3.f0;

				}
				if (batchSum > 0) {
					out.collect(batchSum);
				}
			}
		});

		long start = System.nanoTime();
		System.out.println(modify2.count());
		long end = System.nanoTime();
		System.out.println("total time " + (end - start));
	}
}
