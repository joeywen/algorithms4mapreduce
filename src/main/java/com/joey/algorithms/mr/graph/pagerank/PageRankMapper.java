package com.joey.algorithms.mr.graph.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, ObjectWritable> {
  protected void map(Text key, Text value, Context context)
      throws java.io.IOException, InterruptedException {
    // read information from line
    String URL = key.toString();
    RankRecord record = new RankRecord(value.toString());
    for (String u : record.getUrlList()) {
      // output url rank
      double newRank = record.getUrlList().length == 0 ? 0 : record.getRank()
          / (record.getUrlList().length);
      context.write(new Text(u),
          new ObjectWritable(new DoubleWritable(newRank)));
    }

    // output url list
    context.write(new Text(URL), new ObjectWritable(record.getUrlList()));
  };
}
