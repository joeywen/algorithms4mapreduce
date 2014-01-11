package com.joey.mapred.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 * Writable extension of a Java ArrayList. Elements in the list must be homogeneous and must
 * implement Hadoop's Writable interface.
 * 
 */

public class ArrayListWritable<E extends Writable> extends ArrayList<E> implements Writable {
	/**
	 * Creates an ArrayListWritable object.
	 */
	public ArrayListWritable() {
		super();
	}

	/**
	 * Creates an ArrayListWritable object from an ArrayList.
	 */
	public ArrayListWritable(ArrayList<E> array) {
		super(array);
	}

	/**
	 * Deserializes the array.
	 *
	 * @param in source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numFields = in.readInt();
		if (numFields == 0)
			return;
		String className = in.readUTF();
		E obj;
		try {
			Class<E> c = (Class<E>) Class.forName(className);
			for (int i = 0; i < numFields; i++) {
				obj = (E) c.newInstance();
				obj.readFields(in);
				this.add(obj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Serializes this array.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		if (size() == 0)
			return;
		E obj = get(0);

		out.writeUTF(obj.getClass().getCanonicalName());

		for (int i = 0; i < size(); i++) {
			obj = get(i);
			if (obj == null) {
				throw new IOException("Cannot serialize null fields!");
			}
			obj.write(out);
		}
	}

	/**
	 * Generates human-readable String representation of this ArrayList.
	 *
	 * @return human-readable String representation of this ArrayList
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < this.size(); i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(this.get(i));
		}
		sb.append("]");

		return sb.toString();
	}
}
