package com.joey.algorithms.mr.matrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

import com.joey.algorithms.mr.graph.BaseDriver;

public class TwoStepMatrixMultiplicationDriver extends BaseDriver {

 static class Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] indicesAndValue = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (indicesAndValue[0].equals("A")) {
            outputKey.set(indicesAndValue[2]);
            outputValue.set("A," + indicesAndValue[1] + "," + indicesAndValue[3]);
            context.write(outputKey, outputValue);
        } else {
            outputKey.set(indicesAndValue[1]);
            outputValue.set("B," + indicesAndValue[2] + "," + indicesAndValue[3]);
            context.write(outputKey, outputValue);
        }
    }
}

static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] value;
        ArrayList<HashMap<Integer, Float>> listA = new ArrayList<HashMap<Integer, Float>>();
        ArrayList<HashMap<Integer, Float>> listB = new ArrayList<HashMap<Integer, Float>>();
        for (Text val : values) {
            value = val.toString().split(",");
            if (value[0].equals("A")) {
                listA.add(new HashMap<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
            } else {
                listB.add(new HashMap<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
            }
        }
        String i;
        float a_ij;
        String k;
        float b_jk;
        Text outputValue = new Text();
        Iterator<HashMap<Integer, Float>> iter = listA.iterator();
        while (iter.hasNext()) {
            HashMap<Integer, Float> hash = iter.next();
            i = Integer.toString(hash.g);
            a_ij = a.getValue();
            for (Entry<Integer, Float> b : listB) {
                k = Integer.toString(b.getKey());
                b_jk = b.getValue();
                outputValue.set(i + "," + k + "," + Float.toString(a_ij*b_jk));
                context.write(null, outputValue);
            }
        }
    }
}
  
  public int run(String[] arg0) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {

      @Override
      public Class<?> getJarByClass() {
        return TwoStepMatrixMultiplicationDriver.class;
      }

      @Override
      public Class<? extends Mapper> getMapperClass() {
        return null;
      }

      @Override
      public Class<? extends Reducer> getCombinerClass() {
        return null;
      }

      @Override
      public Class<? extends Reducer> getReducerClass() {
        return null;
      }

      @Override
      public Class<?> getOutputKeyClass() {
        return null;
      }

      @Override
      public Class<?> getOutputValueClass() {
        return null;
      }

      @Override
      public Class<? extends InputFormat> getInputFormatClass() {
        return null;
      }

      @Override
      public Class<? extends OutputFormat> getOutputFormatClass() {
        // TODO Auto-generated method stub
        return null;
      }
      
    };
    return null;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new TwoStepMatrixMultiplicationDriver(), args));
  }
}
