package com.joey.algorithms.mr.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.joey.algorithms.mr.graph.BaseDriver;

public class PageRankDriver extends BaseDriver {

  public static void main(String[] args) throws Exception {
    int res = 0;
    for (int i = 0; i < 25; i++) {
      System.out.println("Round" + i);
      res = ToolRunner.run(new Configuration(), new PageRankDriver(), args);
    }
    System.exit(res);
  }

  public int run(String[] arg0) throws Exception {
    Job job = getJobConf(arg0);

    // get the crawler directory
    String workdir = getConf().get("com.joey.dir", "user/joey/pagerank/");
    FileInputFormat.addInputPath(job, new Path(workdir + "tmp/in"));

    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(workdir + "tmp/out"), true);
    FileOutputFormat.setOutputPath(job, new Path(workdir + "tmp/out"));
    int res = job.waitForCompletion(true) ? 0 : 1;
    if (res == 0) {
      fs.delete(new Path(workdir + "tmp/in"), true);
      fs.rename(new Path(workdir + "tmp/out"), new Path(workdir + "tmp/in"));
    }
    return res;
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {

      @Override
      public Class<?> getJarByClass() {
        return PageRankDriver.class;
      }

      @Override
      public Class<? extends Mapper> getMapperClass() {
        return PageRankMapper.class;
      }

      @Override
      public Class<? extends Reducer> getCombinerClass() {
        return null;
      }

      @Override
      public Class<? extends Reducer> getReducerClass() {
        return PageRankReducer.class;
      }

      @Override
      public Class<?> getOutputKeyClass() {
        return Text.class;
      }

      @Override
      public Class<?> getOutputValueClass() {
        return ObjectWritable.class;
      }

      @Override
      public Class<? extends InputFormat> getInputFormatClass() {
        return KeyValueTextInputFormat.class;
      }

      @Override
      public Class<? extends OutputFormat> getOutputFormatClass() {
        return null;
      }
      
    };
    return super.setupJob("PageRank", jobInfo);
  }

}
