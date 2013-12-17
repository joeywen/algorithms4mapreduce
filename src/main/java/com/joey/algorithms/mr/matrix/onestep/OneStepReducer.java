package com.joey.algorithms.mr.matrix.onestep;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * The output file has one line of the follwing format for 
 * each non-zero element m(i,j) of a matrix M, comma seperated
 * output format: <i><j><m_ij>
 * @author joey.wen@outlook.com
 *
 */
public class OneStepReducer extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String[] value;
    HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
    HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
    for (Text val : values) {
        value = val.toString().split(",");
        if (value[0].equals("A")) {
            hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
        } else {
            hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
        }
    }
    int n = Integer.parseInt(context.getConfiguration().get("n"));
    float result = 0.0f;
    float a_ij;
    float b_jk;
    for (int j = 0; j < n; j++) {
        a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
        b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
        result += a_ij * b_jk;
    }
    if (result != 0.0f) {
        context.write(null, new Text(key.toString() + "," + Float.toString(result)));
    }
}
}
