package com.joey.algorithms.mr.matrix.onestep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * the input file has one line of the following format for each
 * non-zero element m(i,j) of a matrxi M,comma seprated
 * 
 * input format: <M><i><j><m_ij>
 * 
 * @author joey.wen@outlook.com
 *
 */

public class OneStepMapper extends Mapper <LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int m = Integer.parseInt(conf.get("m"));
      int p = Integer.parseInt(conf.get("p"));
      String line = value.toString();
      String[] indicesAndValue = line.split(",");
      Text outputKey = new Text();
      Text outputValue = new Text();
      if (indicesAndValue[0].equals("A")) {
          for (int k = 0; k < p; k++) {
              outputKey.set(indicesAndValue[1] + "," + k);
              outputValue.set("A," + indicesAndValue[2] + "," + indicesAndValue[3]);
              context.write(outputKey, outputValue);
          }
      } else {
          for (int i = 0; i < m; i++) {
              outputKey.set(i + "," + indicesAndValue[2]);
              outputValue.set("B," + indicesAndValue[1] + "," + indicesAndValue[3]);
              context.write(outputKey, outputValue);
          }
      }
  }
}
