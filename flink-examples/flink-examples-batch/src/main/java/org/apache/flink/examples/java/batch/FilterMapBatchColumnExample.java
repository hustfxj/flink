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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.batch.source.BatchInputFormat;
import org.apache.flink.examples.java.batch.source.GenerateBatch;
import org.apache.flink.examples.java.batch.vector.IntegerColumnVector;
import org.apache.flink.examples.java.batch.vector.LongColumnVector;
import org.apache.flink.examples.java.batch.vector.VectorizedRowBatch;
import org.apache.flink.util.Collector;

import static org.apache.flink.examples.java.batch.Utils.BATCH_SIZE;
import static org.apache.flink.examples.java.batch.Utils.DATA_SIZE;

public class FilterMapBatchColumnExample {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		GenerateBatch.GenerateColunmBatch generateColunmBatch = new GenerateBatch.GenerateColunmBatch(BATCH_SIZE);
		BatchInputFormat<VectorizedRowBatch> batchInputFormat = new BatchInputFormat(
			generateColunmBatch,
			DATA_SIZE);

		TypeInformation<VectorizedRowBatch> types = TypeExtractor.createTypeInfo(VectorizedRowBatch.class);

		DataSet<VectorizedRowBatch> source = new DataSource<>(env, batchInputFormat, types,
		                                                      "row-source");

		//filter col0 > 900 & col1 > 900 & col2 > 900
		DataSet<VectorizedRowBatch> filter = source.flatMap(new FlatMapFunction<VectorizedRowBatch, VectorizedRowBatch>() {
			@Override
			public void flatMap(
				VectorizedRowBatch value, Collector<VectorizedRowBatch> out) throws Exception {

				IntegerColumnVector sel1 = new IntegerColumnVector(BATCH_SIZE);
				int ret1 = filterColumnVector(BATCH_SIZE, sel1, (LongColumnVector) value.cols[0], 900, null);

				IntegerColumnVector sel2 = new IntegerColumnVector(ret1);
				int ret2 = filterColumnVector(ret1, sel2, (LongColumnVector) value.cols[1], 900, sel1);

				IntegerColumnVector sel3 = new IntegerColumnVector(ret2);
				int ret3 = filterColumnVector(ret2, sel3, (LongColumnVector) value.cols[2], 900, sel2);

				value.selected = new int[ret3];
				for (int i = 0; i < ret3; i++) {
					value.selected[i] = sel3.vector[0];
				}
				out.collect(value);
			}
		});

		//modify-1 "col0 +1" and "col2 - 1"
		DataSet<VectorizedRowBatch> modify1 = filter.flatMap(new FlatMapFunction<VectorizedRowBatch, VectorizedRowBatch>() {

			@Override
			public void flatMap(
				VectorizedRowBatch value, Collector<VectorizedRowBatch> out) throws Exception {
				addColumnVector(value.selected, (LongColumnVector) value.cols[0], 1);
				addColumnVector(value.selected, (LongColumnVector) value.cols[2], -1);
				out.collect(value);
			}
		});

		//modify-2 "select col2"
		DataSet<Long> modify2 = filter.flatMap(new FlatMapFunction<VectorizedRowBatch,
			Long>() {
			@Override
			public void flatMap(VectorizedRowBatch value, Collector<Long> out) throws Exception {
				long batchSum = 0;
				for (int i = 0; i < value.selected.length; i++) {
					int index = value.selected[i];
					batchSum += ((LongColumnVector) value.cols[2]).vector[index];
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

	public static int filterColumnVector(
		int n, IntegerColumnVector ret, LongColumnVector input, long val,
		IntegerColumnVector lastRet) {
		int j = 0;
		if (lastRet == null) {
			for (int i = 0; i < n; i++) {
				ret.vector[j] = i;
				j += (input.vector[i] > val) ? 1 : 0;
			}
		} else {
			for (int i = 0; i < n; i++) {
				ret.vector[j] = lastRet.vector[i];
				j += (input.vector[lastRet.vector[i]] > val) ? 1 : 0;
			}
		}
		return j;
	}

	public static void addColumnVector(int[] selected, LongColumnVector input, long val) {
		for (int i = 0; i < selected.length; i++) {
			input.vector[selected[i]] += val;
		}
	}
}
