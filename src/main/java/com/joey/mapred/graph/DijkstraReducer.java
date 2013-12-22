package com.joey.mapred.graph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DijkstraReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
  public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    //From slide 20 of Graph Algorithms with MapReduce (by Jimmy Lin, Univ @ Maryland)
    //The key is the current point
    //The values are all the possible distances to this point
    //we simply emit the point and the minimum distance value

    String nodes = "UNMODED";
    Text word = new Text();
    int lowest = 10009;//start at infinity

    for (Text val : values) {//looks like NODES/VALUES 1 0 2:3:, we need to use the first as a key
        String[] sp = val.toString().split(" ");//splits on space
        //look at first value
        if(sp[0].equalsIgnoreCase("NODES")){
            nodes = null;
            nodes = sp[1];
        }else if(sp[0].equalsIgnoreCase("VALUE")){
            int distance = Integer.parseInt(sp[1]);
            lowest = Math.min(distance, lowest);
        }
    }
    word.set(lowest+" "+nodes);
    context.write(key, word);
    word.clear();
}
}
