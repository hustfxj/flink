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

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import java.io.IOException;


public class BatchInputFormat<T> extends GenericInputFormat<T> implements NonParallelInput {

	private static final long serialVersionUID = 10897737445L;

	private long dataSize;
	private GenerateBatch<T> generateBatch;


	public BatchInputFormat(GenerateBatch<T> generateBatch, long dataSize) {

		if (dataSize < 1) {
			throw new IllegalArgumentException("size has to be at least 1.");
		}

		this.dataSize = dataSize;
		this.generateBatch = generateBatch;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return dataSize == 0;
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
	}

	@Override
	public T nextRecord(T record) throws IOException {
		dataSize--;
		return this.generateBatch.generator();
	}

}
