package org.apache.flink.examples.java.batch.vector;

import java.util.Arrays;

/**
 * @author xiaojian.fxj
 * @since 17/7/8
 */
public class IntegerColumnVector extends ColumnVector {
	public int[] vector;
	public static final int NULL_VALUE = 1;

	/**
	 * Use this constructor by default. All column vectors
	 * should normally be the default size.
	 */
	public IntegerColumnVector() {
		this(VectorizedRowBatch.DEFAULT_SIZE);
	}

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public IntegerColumnVector(int len) {
		super(len);
		vector = new int[len];
	}

	// Copy the current object contents into the output. Only copy selected entries,
	// as indicated by selectedInUse and the sel array.
	public void copySelected(
		boolean selectedInUse, int[] sel, int size, LongColumnVector output) {

		// Output has nulls if and only if input has nulls.
		output.noNulls = noNulls;
		output.isRepeating = false;

		// Handle repeating case
		if (isRepeating) {
			output.vector[0] = vector[0];
			output.isNull[0] = isNull[0];
			output.isRepeating = true;
			return;
		}

		// Handle normal case

		// Copy data values over
		if (selectedInUse) {
			for (int j = 0; j < size; j++) {
				int i = sel[j];
				output.vector[i] = vector[i];
			}
		}
		else {
			System.arraycopy(vector, 0, output.vector, 0, size);
		}

		// Copy nulls over if needed
		if (!noNulls) {
			if (selectedInUse) {
				for (int j = 0; j < size; j++) {
					int i = sel[j];
					output.isNull[i] = isNull[i];
				}
			}
			else {
				System.arraycopy(isNull, 0, output.isNull, 0, size);
			}
		}
	}

	// Copy the current object contents into the output. Only copy selected entries,
	// as indicated by selectedInUse and the sel array.
	public void copySelected(
		boolean selectedInUse, int[] sel, int size, DoubleColumnVector output) {

		// Output has nulls if and only if input has nulls.
		output.noNulls = noNulls;
		output.isRepeating = false;

		// Handle repeating case
		if (isRepeating) {
			output.vector[0] = vector[0];  // automatic conversion to double is done here
			output.isNull[0] = isNull[0];
			output.isRepeating = true;
			return;
		}

		// Handle normal case

		// Copy data values over
		if (selectedInUse) {
			for (int j = 0; j < size; j++) {
				int i = sel[j];
				output.vector[i] = vector[i];
			}
		}
		else {
			for(int i = 0; i < size; ++i) {
				output.vector[i] = vector[i];
			}
		}

		// Copy nulls over if needed
		if (!noNulls) {
			if (selectedInUse) {
				for (int j = 0; j < size; j++) {
					int i = sel[j];
					output.isNull[i] = isNull[i];
				}
			}
			else {
				System.arraycopy(isNull, 0, output.isNull, 0, size);
			}
		}
	}

	// Fill the column vector with the provided value
	public void fill(int value) {
		noNulls = true;
		isRepeating = true;
		vector[0] = value;
	}

	// Fill the column vector with nulls
	public void fillWithNulls() {
		noNulls = false;
		isRepeating = true;
		vector[0] = NULL_VALUE;
		isNull[0] = true;
	}

	// Simplify vector by brute-force flattening noNulls and isRepeating
	// This can be used to reduce combinatorial explosion of code paths in VectorExpressions
	// with many arguments.
	public void flatten(boolean selectedInUse, int[] sel, int size) {
		flattenPush();
		if (isRepeating) {
			isRepeating = false;
			int repeatVal = vector[0];
			if (selectedInUse) {
				for (int j = 0; j < size; j++) {
					int i = sel[j];
					vector[i] = repeatVal;
				}
			} else {
				Arrays.fill(vector, 0, size, repeatVal);
			}
			flattenRepeatingNulls(selectedInUse, sel, size);
		}
		flattenNoNulls(selectedInUse, sel, size);
	}

	@Override
	public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
		if (inputVector.isRepeating) {
			inputElementNum = 0;
		}
		if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
			isNull[outElementNum] = false;
			vector[outElementNum] =
				((IntegerColumnVector) inputVector).vector[inputElementNum];
		} else {
			isNull[outElementNum] = true;
			noNulls = false;
		}
	}

	@Override
	public void stringifyValue(StringBuilder buffer, int row) {
		if (isRepeating) {
			row = 0;
		}
		if (noNulls || !isNull[row]) {
			buffer.append(vector[row]);
		} else {
			buffer.append("null");
		}
	}

	@Override
	public void ensureSize(int size, boolean preserveData) {
		super.ensureSize(size, preserveData);
		if (size > vector.length) {
			int[] oldArray = vector;
			vector = new int[size];
			if (preserveData) {
				if (isRepeating) {
					vector[0] = oldArray[0];
				} else {
					System.arraycopy(oldArray, 0, vector, 0 , oldArray.length);
				}
			}
		}
	}

	@Override
	public void shallowCopyTo(ColumnVector otherCv) {
		IntegerColumnVector other = (IntegerColumnVector)otherCv;
		super.shallowCopyTo(other);
		other.vector = vector;
	}
}
