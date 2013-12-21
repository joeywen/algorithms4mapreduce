package com.joey.algorithms.mr.graph.mst;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.joey.algorithms.mr.graph.BaseDriver;

public class MSTDriver extends BaseDriver {

  static enum MSTCounters {
    totalWeight
  }
  
  /**
   * Description: MSTMapper class that emits the input records in the form of
   * <key, value> pairs where the key is the weight and the value is the source
   * ,destination pair. Mapreduce has automatic sorting by keys after the map phase
   * This property can be used to sort the weights of the given graph.
   * Therefore we hava a mapper, the output of which will have the data sorted by keys
   * which are weights in this program. So the reducer will get the edges in the order
   * of increasing weight.
   * 
   * Input format: <key, value>: <automatically assigned key, weight<tab>source<tab>destination>
   * 
   * Output format: <key, value>:<weight, source:destination>
   *
   */
  class MSTMapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      Text srcDestPair = new Text();
      String[] inputTokens = value.toString().split("\t");
      
      String weight = inputTokens[0];
      int wt = Integer.parseInt(weight);
      IntWritable iwWeight = new IntWritable(wt);
      srcDestPair.set(inputTokens[1] + ":" + inputTokens[2]);
      
      context.write(iwWeight, srcDestPair);
    }
    
  }
  
  /**
   * Reference:
   *  http://www.personal.kent.edu/~rmuhamma/Algorithms/MyAlgorithms/GraphAlgor/kruskalAlgor.htm
   * Kruskal's algorithm : the MST is built as a forest. The edges are sorted by wieghts
   * and considered in the increasing order of the weights. Each vertex is in its tree in the forest
   * Each edge is taken and if the end nodes of the edge belong to disjoint tress, then they are merged
   * and the edge is considered to be in the MST. Else,  the edge is discarded.
   *
   */
   class MSTReducer extends Reducer<IntWritable, Text, Text, Text> {

    Map<String, Set<String>> node_AssociatedSet = new HashMap<String, Set<String>>();
    
    private boolean unionSet(Set<String> nodesSet, String node1, String node2) {
      boolean ignoreEdge = false;
      if (!node_AssociatedSet.containsKey(node1)) {
        node_AssociatedSet.put(node1, nodesSet);
      } else {
        Set<String> associatedSet = node_AssociatedSet.get(node1);
        Set<String> nodeSet = new HashSet<String>();
        
        nodeSet.addAll(associatedSet);
        Iterator<String> nodeItr = nodeSet.iterator();
        Iterator<String> duplicateCheckItr = nodeSet.iterator();
        
        while (duplicateCheckItr.hasNext()) {
          String n = duplicateCheckItr.next();
          if (node_AssociatedSet.get(n).contains(node2)) {
            ignoreEdge = true;
          }
        }
        
        while (nodeItr.hasNext()) {
          String nextNode = nodeItr.next();
          if (!node_AssociatedSet.containsKey(nextNode)) {
            node_AssociatedSet.put(nextNode, nodesSet);
          }
          
          node_AssociatedSet.get(nextNode).addAll(nodeSet);
        }
      }
      
      return ignoreEdge;
    }
    
    private boolean isSameSet(String src, String dest) {
      boolean ignoreEdge = false;
      for (Map.Entry<String, Set<String>> node_AssociatedSetValue :
        node_AssociatedSet.entrySet()) {
        Set<String> nodesInSameSet = node_AssociatedSetValue.getValue();
        if (nodesInSameSet.contains(src) && nodesInSameSet.contains(dest)) {
          ignoreEdge = true;
        }
      }
      
      return ignoreEdge;
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      String strKey = new String();
      strKey += key;
      Text outputKey = new Text(strKey);
      
      for (Text val : values) {
        boolean ignoreEdgeSameSet1 = false;
        boolean ignoreEdgeSmaeSet2 = false;
        boolean ignoreEdgeSameSet3 = false;
        
        Set<String> nodeSet = new HashSet<String>();
        
        String[] srcDest = val.toString().split(":");
        
        String src = srcDest[0];
        String dest = srcDest[1];
        
        ignoreEdgeSameSet1 = isSameSet(src, dest);
        
        nodeSet.add(src);
        nodeSet.add(dest);
        
        ignoreEdgeSmaeSet2 = unionSet(nodeSet, src, dest);
        ignoreEdgeSameSet3 = unionSet(nodeSet, dest, src);
        
        if (!ignoreEdgeSameSet1 && !ignoreEdgeSameSet3 && !ignoreEdgeSmaeSet2) {
          long weight = Long.parseLong(outputKey.toString());
          context.getCounter(MSTCounters.totalWeight).increment(weight);
          context.write(outputKey, val);
        }
      }
    }
    
  }
  
  public int run(String[] args) throws Exception {
    Job mstJob = getJobConf(args);
    
    FileInputFormat.setInputPaths(mstJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(mstJob, new Path(args[1]));
    
    boolean ret = mstJob.waitForCompletion(true);
    
    Counters jobCnters = mstJob.getCounters();
    long totalWeight = jobCnters.findCounter(MSTCounters.totalWeight).getValue();
    System.out.println("The total weight of the MST is " + totalWeight);
    
    return ret ? 0 : 1;
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {
      @Override
      public Class<? extends Reducer> getCombinerClass() {
        
        return null;
      }

      @Override
      public Class<?> getJarByClass() {
        return MSTDriver.class;
      }

      @Override
      public Class<? extends Mapper> getMapperClass() {
        return MSTMapper.class;
      }

      @Override
      public Class<?> getOutputKeyClass() {
        return IntWritable.class;
      }

      @Override
      public Class<?> getOutputValueClass() {
        return Text.class;
      }

      @Override
      public Class<? extends Reducer> getReducerClass() {
        return MSTReducer.class;
      }

      @Override
      public Class<? extends InputFormat> getInputFormatClass() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Class<? extends OutputFormat> getOutputFormatClass() {
        // TODO Auto-generated method stub
        return null;
      }
    };
    
    return setupJob("formMST", jobInfo);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MSTDriver(), args);
    if (args.length != 2) {
      System.err
          .println("Usage: MST <in> <output > ");
      System.exit(2);
    }
    System.exit(res);
  }

}
