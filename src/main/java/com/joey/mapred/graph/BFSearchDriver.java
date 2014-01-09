package com.joey.mapred.graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
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

import com.joey.mapred.BaseDriver;
import com.joey.mapred.graph.TraverseGraph.TraverseMapper;
import com.joey.mapred.graph.TraverseGraph.TraverseReducer;
import com.joey.mapred.graph.utils.Node;

/**
 * Description : MapReduce program to solve the single-source shortest path
 * problem using parallel breadth-first search. This program also illustrates
 * how to perform iterative map-reduce.
 * 
 * The single source shortest path is implemented by using Breadth-first search
 * concept.
 * 
 * Reference :
 * http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search
 * -using-iterative-map-reduce-algorithm
 * 
 */

public class BFSearchDriver extends BaseDriver {

  static class SearchMapperSSSP extends TraverseMapper {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      Node inNode = new Node(value.toString());
      // calls the map method of the super class SearchMapper
      super.map(key, value, context, inNode);

    }
  }
  
  
  static class SearchReducerSSSP extends TraverseReducer {

    // the parameters are the types of the input key, the values associated with
    // the key and the Context object through which the Reducer communicates
    // with the Hadoop framework

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      // create a new out node and set its values
      Node outNode = new Node();

      // call the reduce method of SearchReducer class
      outNode = super.reduce(key, values, context, outNode);

      // if the color of the node is gray, the execution has to continue, this
      // is done by incrementing the counter
      if (outNode.getColor() == Node.Color.GRAY)
        context.getCounter(MoreIterations.numberOfIterations).increment(1L);
    }
  }
  
  public int run(String[] args) throws Exception {
    int iterationCount = 0; // counter to set the ordinal number of the
                            // intermediate outputs
    Job job;
    long terminationValue = 1;
    
    // while there are more gray nodes to process
    while (terminationValue > 0) {
      job = getJobConf(args); // get the job configuration
      String input, output;

      // setting the input file and output file for each iteration
      // during the first time the user-specified file will be the input whereas
      // for the subsequent iterations
      // the output of the previous iteration will be the input
      if (iterationCount == 0) { 
      	// for the first iteration the input will be the first input argument
        input = args[0];
      } else {
        // for the remaining iterations, the input will be the output of the
        // previous iteration
        input = args[1] + iterationCount;
      }
      output = args[1] + (iterationCount + 1); // setting the output file

      FileInputFormat.setInputPaths(job, new Path(input)); 
      FileOutputFormat.setOutputPath(job, new Path(output)); 

      job.waitForCompletion(true); 

      Counters jobCntrs = job.getCounters();
      terminationValue = jobCntrs
          .findCounter(MoreIterations.numberOfIterations).getValue();
      iterationCount++;
    }

    return 0;
  }
  
  static enum MoreIterations {
    numberOfIterations
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BFSearchDriver(), args);
    if(args.length != 2){
      System.err.println("Usage: <in> <output name> ");
    }
    System.exit(res);
  }


  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {

      @Override
      public Class<?> getJarByClass() {
        return BFSearchDriver.class;
      }

      @Override
      public Class<? extends Mapper> getMapperClass() {
        return SearchMapperSSSP.class;
      }

      @Override
      public Class<? extends Reducer> getCombinerClass() {
        return null;
      }

      @Override
      public Class<? extends Reducer> getReducerClass() {
        return SearchReducerSSSP.class;
      }

      @Override
      public Class<?> getOutputKeyClass() {
        return Text.class;
      }

      @Override
      public Class<?> getOutputValueClass() {
        return Text.class;
      }

      @Override
      public Class<? extends InputFormat> getInputFormatClass() {
        return TextInputFormat.class;
      }

      @Override
      public Class<? extends OutputFormat> getOutputFormatClass() {
        return TextOutputFormat.class;
      }
      
    };
    return setupJob("BFSSearch", jobInfo);
  }

}
