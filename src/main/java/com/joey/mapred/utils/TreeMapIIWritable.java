package com.joey.mapred.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TreeMapIIWritable extends TreeMap<IntWritable, IntWritable> implements
    Writable {

	public void write(DataOutput out) throws IOException {
		// write out the number of entries
		out.writeInt(size());
		// output each entry pair
		for (Map.Entry<IntWritable, IntWritable> entry : entrySet()) {
			entry.getKey().write(out);
			entry.getValue().write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		// clear current contents - hadoop re-uses objects
		// between calls to your map / reduce methods
		clear();

		// read how many items to expect
		int count = in.readInt();
		// deserialize a key and value pair, insert into map
		while (count-- > 0) {
			IntWritable key = new IntWritable();
			key.readFields(in);

			IntWritable value = new IntWritable();
			value.readFields(in);

			put(key, value);
		}
	}
}
