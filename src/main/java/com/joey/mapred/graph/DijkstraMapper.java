package com.joey.mapred.graph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * input format:  node<whitespace>weight<whitespace>adj_list split by ':'
 * 1 0 2:3:
 * 2 10000 1:4:5:
 * 3 10000 1:
 * 4 10000 2:5:
 * 5 10000 2:4:
 * @author Joey
 *
 */
public class DijkstraMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //From slide 20 of Graph Algorithms with MapReduce (by Jimmy Lin, Univ @ Maryland)
      //Key is node n
      //Value is D, Points-To
      //For every point (or key), look at everything it points to.
      //Emit or write to the points to variable with the current distance + 1
      Text word = new Text();
      String line = value.toString();//looks like 1 0 2:3:
      String[] sp = line.split(" ");//splits on space
      int distanceadd = Integer.parseInt(sp[1]) + 1;
      String[] PointsTo = sp[2].split(":");
      for(int i=0; i<PointsTo.length; i++){
          word.set("VALUE "+distanceadd);//tells me to look at distance value
          context.write(new LongWritable(Integer.parseInt(PointsTo[i])), word);
          word.clear();
      }
      //pass in current node's distance (if it is the lowest distance)
      word.set("VALUE "+sp[1]);
      context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
      word.clear();

      word.set("NODES "+sp[2]);//tells me to append on the final tally
      context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
      word.clear();
  }
}