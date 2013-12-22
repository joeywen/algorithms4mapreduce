package com.joey.mapred.recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import com.joey.mapred.graph.BaseDriver;

public class FriendRecommendationDriver extends BaseDriver {

  static class FriendRecMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line  = value.toString();
      String[] userAndFriend = line.split("\t");
      if (userAndFriend.length == 2) {
        String user = userAndFriend[0];
        IntWritable userKey = new IntWritable(Integer.parseInt(user));
        String[] friends = userAndFriend[1].split(",");
        String friend1;
        IntWritable friend1Key = new IntWritable();
        Text friend1Value = new Text();
        String friend2;
        IntWritable friend2Key = new IntWritable();
        Text friend2Value = new Text();
        
        for (int i = 0; i < friends.length; ++i) {
          friend1 = friends[i];
          friend1Value.set("1," + friend1);
          context.write(userKey, friend1Value); // Paths of length 1
          friend1Key.set(Integer.parseInt(friend1));
          friend1Value.set("2," + friend1);
          
          for (int j = i + 1; j < friends.length; ++j) {
            friend2 = friends[j];
            friend2Key.set(Integer.parseInt(friend2));
            friend2Value.set("2," + friend2);
            context.write(friend2Key, friend2Value);// Paths of length 2
            context.write(friend2Key, friend1Value);// Paths of length 2
          }
        }
      }
    }
    
  }
  
  static class FriendRecReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      String[] value;
      HashMap<String, Integer> hash = new HashMap<String, Integer>();
      for (Text val : values) {
        value = (val.toString()).split(",");
        if (value[0].equals("1")) { // Paths length of 1
          hash.put(value[1], -1);
        } else if (value[0].equals("2")){
          // Paths length of 2
          if (hash.containsKey(value[1])) {
            if (hash.get(value[1]) != -1) {
              hash.put(value[1], hash.get(value[1]) + 1);
            } else {
              hash.put(value[1], 1);
            }
          }
        }
      } // end of for
      
      // Convert hash to list and remove paths of length 1
      ArrayList<Entry<String, Integer>> list = new ArrayList<Map.Entry<String,Integer>>();
      for (Entry<String, Integer> entry : hash.entrySet()) {
        if (entry.getValue() != -1) { // exclude paths of length 1
          list.add(entry);
        }
      } // end of for
      
      // sort key-value pairs in the list by values (number of common friends).
      Collections.sort(list, new Comparator<Entry<String, Integer>> () {

        public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
          return o2.getValue().compareTo(o1.getValue());
        }
        
      });
      
      int MAX_RECOMMENDATION_COUNT = context.getConfiguration().getInt("max.recommendation.count", 10);
      if (MAX_RECOMMENDATION_COUNT < 1) {
        context.write(key, new Text(StringUtils.join(list, ",")));
      } else {
        ArrayList<String> top = new ArrayList<String>();
        for (int i = 0; i < Math.min(MAX_RECOMMENDATION_COUNT, list.size()); ++i) {
          top.add(list.get(i).getKey());
        }
        
        context.write(key, new Text(StringUtils.join(top, ",")));
      }
    }
    
  }
  
  public int run(String[] args) throws Exception {
    Job job = getJobConf(args);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {

      @Override
      public Class<?> getJarByClass() {
        return FriendRecommendationDriver.class;
      }

      @Override
      public Class<? extends Mapper> getMapperClass() {
        return FriendRecMapper.class;
      }

      @Override
      public Class<? extends Reducer> getCombinerClass() {
        return null;
      }

      @Override
      public Class<? extends Reducer> getReducerClass() {
        return FriendRecReducer.class;
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
    return super.setupJob("friend-recommendation", jobInfo);
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new FriendRecommendationDriver(), args));
  }

}
