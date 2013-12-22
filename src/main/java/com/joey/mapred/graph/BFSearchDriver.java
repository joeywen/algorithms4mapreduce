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

import com.joey.mapred.graph.utils.Node;
import com.joey.mapred.graph.utils.Node.Color;

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

  /**
   * 
   * Description : Mapper class that implements the map part of Breadth-first
   * search algorithm. The nodes colored WHITE or BLACK are emitted as such. For
   * each node that is colored GRAY, a new node is emitted with the distance
   * incremented by one and the color set to GRAY. The original GRAY colored
   * node is set to BLACK color and it is also emitted.
   * 
   * Input format <key, value> : <line offset in the input file (automatically
   * assigned),
   * nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent>
   * 
   * Output format <key, value> : <nodeId, (updated)
   * list_of_adjacent_nodes|distance_from_the_source|color|parent node>
   * 
   * Reference :
   * http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search
   * -using-iterative-map-reduce-algorithm
   * 
   * 
   */

  // the type parameters are the input keys type, the input values type, the
  // output keys type, the output values type
  static class BFSSeachMapper extends Mapper<Object, Text, Text, Text> {

    protected void map(Object key, Text value, Context context, Node inNode)
        throws IOException, InterruptedException {
      if (inNode.getColor() == Color.GRAY) {
        for (String neighbor : inNode.getEdges()) {
          Node adjacentNode = new Node();
          adjacentNode.setId(neighbor);
          adjacentNode.setDistance(inNode.getDistance() + 1);
          adjacentNode.setColor(Node.Color.GRAY);
          adjacentNode.setParent(inNode.getId());
          
          context.write(new Text(adjacentNode.getId()),
              adjacentNode.getNodeInfo());
        }
        // this node is done, color it black
        inNode.setColor(Node.Color.BLACK);
      }

      context.write(new Text(inNode.getId()), inNode.getNodeInfo());
    }
    
  }
  
  static class SearchMapperSSSP extends BFSSeachMapper {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

      Node inNode = new Node(value.toString());
      // calls the map method of the super class SearchMapper
      super.map(key, value, context, inNode);

    }
  }
  
  
  /**
   * 
   * Description : Reducer class that implements the reduce part of parallel
   *  Breadth-first search algorithm. Make a new node which combines all
   *  information for this single node id that is for each key. The new node
   *  should have the full list of edges, the minimum distance, the darkest
   *  Color, and the parent/predecessor node
   * 
   * Input format <key,value> : <nodeId, list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
   * 
   * Output format <key,value> : <nodeId, (updated)list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
   * 
   */
  static class BFSSearchReducer extends Reducer<Text, Text, Text, Text> {

    protected Node reduce(Text key, Iterable<Text> values, Context context,
        Node outNode) throws IOException, InterruptedException {
      // set the node id as the key
      outNode.setId(key.toString());

      for (Text value : values) {
        Node inNode = new Node(key.toString() + "\t" + value.toString());

        if (inNode.getEdges().size() > 0) {
          outNode.setEdges(inNode.getEdges());
        }

        if (inNode.getDistance() < outNode.getDistance()) {
          outNode.setDistance(inNode.getDistance());
          outNode.setParent(inNode.getParent());
        }

        if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {
          outNode.setColor(inNode.getColor());
        }

      }

      context.write(key, new Text(outNode.getNodeInfo()));

      return outNode;
    }

  }
  
  static class SearchReducerSSSP extends BFSSearchReducer {

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
      if (iterationCount == 0) // for the first iteration the input will be the
                               // first input argument
        input = args[0];
      else
        // for the remaining iterations, the input will be the output of the
        // previous iteration
        input = args[1] + iterationCount;

      output = args[1] + (iterationCount + 1); // setting the output file

      FileInputFormat.setInputPaths(job, new Path(input)); // setting the input
                                                           // files for the job
      FileOutputFormat.setOutputPath(job, new Path(output)); // setting the
                                                             // output files for
                                                             // the job

      job.waitForCompletion(true); // wait for the job to complete

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
