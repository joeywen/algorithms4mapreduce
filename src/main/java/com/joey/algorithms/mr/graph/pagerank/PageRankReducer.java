package com.joey.algorithms.mr.graph.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends
    Reducer<Text, ObjectWritable, Text, RankRecord> {

  protected void reduce(Text key, Iterable<ObjectWritable> val, Context context)
      throws java.io.IOException, InterruptedException {
    double d = context.getConfiguration().getFloat("com.joey.pagerank.d",
        (float) .5);
    double rank = .0;

    String[] urlList = new String[0];
    for (ObjectWritable w : val) {
      if (w.getDeclaredClass().toString()
          .equals(DoubleWritable.class.toString())) {
        rank += d * ((DoubleWritable) w.get()).get();
      }
      if (w.getDeclaredClass().toString().equals(String[].class.toString())) {
        urlList = (String[]) w.get();
      }
    }
    rank = (1 - d) + rank;
    context.write(key, new RankRecord(rank, urlList));
  };
}
