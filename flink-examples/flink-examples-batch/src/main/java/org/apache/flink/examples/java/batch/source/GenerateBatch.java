/**
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

package org.apache.flink.examples.java.batch.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.examples.java.batch.vector.LongColumnVector;
import org.apache.flink.examples.java.batch.vector.VectorizedRowBatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public interface GenerateBatch<T> {

	 Random random = new Random(1000);

	T generator();

	class GenerateRowBatch implements GenerateBatch<List<Tuple3<Long, Long, Long>>>, Serializable {
		private final int batchSize;

		public GenerateRowBatch(int batchSize) {
			this.batchSize = batchSize;
		}

		@Override
		public List<Tuple3<Long, Long, Long>> generator() {
			List<Tuple3<Long, Long, Long>> tuple3s = new ArrayList<>(batchSize);
			for (int i = 0; i < batchSize; i++) {
				Tuple3<Long, Long, Long> tuple3 = new Tuple3<>(random.nextLong(), random.nextLong(), random.nextLong());
				tuple3s.add(tuple3);
			}
			return tuple3s;
		}
	}

	class GenerateColunmBatch implements GenerateBatch<VectorizedRowBatch>, Serializable{

		private final int batchSize;

		public GenerateColunmBatch(int batchSize) {
			this.batchSize = batchSize;
		}

		@Override
		public VectorizedRowBatch generator() {
			VectorizedRowBatch batch = new VectorizedRowBatch(3);
			LongColumnVector col1 = new LongColumnVector(batchSize);
			LongColumnVector col2 = new LongColumnVector(batchSize);
			LongColumnVector col3 = new LongColumnVector(batchSize);
			for (int i = 0; i < batchSize; i++) {
				col1.vector[i] = random.nextLong();
				col2.vector[i] = random.nextLong();
				col3.vector[i] = random.nextLong();
			}
			batch.cols[0] = col1;
			batch.cols[1] = col2;
			batch.cols[2] = col3;
			return batch;
		}
	}

}

