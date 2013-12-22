package com.joey.mapred.graph;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


/**
 * graph example
 * 
 * 1 --- 3
 * |
 * |
 * 2 --- 4
 * \     |
 *  \    |
 *   \   |
 *    \  |
 *      5
 * 
 * @author Joey
 *
 */

public class DijkstraDriver extends BaseDriver {

  public static String OUT = "/user/joey/input/";
  public static String IN = "/user/joey/output";
  
  public int run(String[] args) throws Exception {
    getConf().set("mapred.textoutputformat.separator", " ");

    // set in and out to args.
    IN = args[0];
    OUT = args[1];

    String infile = IN;
    String outputfile = OUT + System.nanoTime();

    boolean isDone = false;
    boolean success = false;

    HashMap<Integer, Integer> nextMRDataMap = new HashMap<Integer, Integer>();

    while (isDone == false) {
      Job job = getJobConf(args);

      FileInputFormat.addInputPath(job, new Path(infile));
      FileOutputFormat.setOutputPath(job, new Path(outputfile));

      success = job.waitForCompletion(true);

      // remove the input file
      if (infile != IN) {
        String indir = infile.replace("part-r-00000", "");
        Path ddir = new Path(indir);
        FileSystem dfs = FileSystem.get(getConf());
        dfs.delete(ddir, true);
      }

      infile = outputfile + "/part-r-00000";
      outputfile = OUT + System.nanoTime();

      // do we need to re-run the job with the new input file??
      isDone = true;// set the job to NOT run again!
      Path ofile = new Path(infile);
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(
          fs.open(ofile)));

      HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
      String line = br.readLine();
      while (line != null) {
        // each line looks like 0 1 2:3:
        // we need to verify node -> distance doesn't change
        String[] sp = line.split(" ");
        int node = Integer.parseInt(sp[0]);
        int distance = Integer.parseInt(sp[1]);
        imap.put(node, distance);
        line = br.readLine();
      }
      if (nextMRDataMap.isEmpty()) {
        // first iteration... must do a second iteration regardless!
        isDone = false;
      } else {
        Iterator<Integer> itr = imap.keySet().iterator();
        while (itr.hasNext()) {
          int key = itr.next();
          int val = imap.get(key);
          if (nextMRDataMap.get(key) != val) {
            // values aren't the same... we aren't at convergence yet
            isDone = false;
          }
        }
      }
      if (isDone == false) {
        nextMRDataMap.putAll(imap);// copy imap to _map for the next iteration (if
                          // required)
      }
    }

    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DijkstraDriver(), args));
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {

      public Class<?> getJarByClass() {
        return DijkstraDriver.class;
      }

      public Class<? extends Mapper> getMapperClass() {
        return DijkstraMapper.class;
      }

      public Class<? extends Reducer> getCombinerClass() {
        return null;
      }

      public Class<? extends Reducer> getReducerClass() {
        return DijkstraReducer.class;
      }

      public Class<?> getOutputKeyClass() {
        return LongWritable.class;
      }

      public Class<?> getOutputValueClass() {
        return Text.class;
      }

      @Override
      public Class<? extends InputFormat> getInputFormatClass() {
        // TODO Auto-generated method stub
        return TextInputFormat.class;
      }

      @Override
      public Class<? extends OutputFormat> getOutputFormatClass() {
        // TODO Auto-generated method stub
        return TextOutputFormat.class;
      }
      
    };
    
    return setupJob("DijkstraGraph", jobInfo);
  }

}
