package com.joey.mapred.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AllSimplePathsFinder {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
			String line = value.toString();
			String[] nodes = line.split(",");
			if (nodes.length >= 2) {
				String firstNode = nodes[0];
				String[] restOfPath = Arrays.copyOfRange(nodes, 1, nodes.length);
				context.write(new Text(firstNode), new Text(StringUtils.join(restOfPath, ",")));
				
				Collections.reverse(Arrays.asList(nodes));
				firstNode = nodes[0];
				restOfPath = Arrays.copyOfRange(nodes, 1, nodes.length);
				context.write(new Text(firstNode), new Text(StringUtils.join(restOfPath, ",")));
			}
    }
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
    protected void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException,
        InterruptedException {
			ArrayList<ArrayList<String>> paths = new ArrayList<ArrayList<String>>();
			for (Text val : values) {
				paths.add(new ArrayList<String>(Arrays.asList(val.toString().split(","))));
				
				//remove duplicates
				LinkedHashSet<ArrayList<String>> set = new LinkedHashSet<ArrayList<String>>(paths);
				paths.clear();
				paths.addAll(set);
				
				//output original and new paths
				ArrayList<String> nodes = new ArrayList<String>();
				ArrayList<String> node1 = new ArrayList<String>();
				ArrayList<String> node2 = new ArrayList<String>();
				
				for (int i = 0; i < paths.size(); ++i) {
					node1 = paths.get(i);
					nodes.clear();
					nodes.add(key.toString());
					nodes.addAll(node1);
					
					context.write(null, new Text(StringUtils.join(nodes, ",")));
					
					for (int j = i+1; j < paths.size(); ++j) {
						node2 = paths.get(j);
						nodes.clear();
						nodes.addAll(node1);
						nodes.retainAll(node2);
						
						if (nodes.isEmpty()) {
							// node1 and node2 don't have a common node
							nodes.addAll(node1);
							Collections.reverse(nodes);
							nodes.add(key.toString());
							nodes.addAll(node2);
							context.write(null, new Text(StringUtils.join(nodes, ",")));
						}
					}
				}
			}
    }
		
	}
	
	public static void main(String[] args) throws Exception {
	  Configuration config = new Configuration();
	  
	  Job job = new Job(config, "PathFinder");
	  job.setJarByClass(AllSimplePathsFinder.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  
	  job.setMapperClass(Map.class);
	  job.setReducerClass(Reduce.class);
	  
	  job.setOutputFormatClass(TextInputFormat.class);
	  job.setInputFormatClass(TextInputFormat.class);
	  
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
	  job.waitForCompletion(true);
  }
}
