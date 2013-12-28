package com.joey.mapred.matrix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.joey.mapred.BaseDriver;

public class WordCoourrenceMatricesDriver extends BaseDriver {

  public static class WordCoourenceMapper extends Mapper<IntWritable, Text, Text, HashMap<Text, IntWritable>> {

    
    
    @Override
    protected void map(IntWritable key/*doc id*/, Text value/*doc content*/, Context context)
        throws IOException, InterruptedException {
      // neighbor threshold
      int distanceThreshold = context.getConfiguration().getInt("neighborThreshold", 4);
      
      HashMap<Text, IntWritable> associativeArray = null;
      
      // the value is the doc context processed by 3rd tokennized tool to split 
      // the sentence to term with whitespace seprated.
      String[] terms = value.toString().split(" ");
      int size = terms.length;
      for (int i = 0; i < size; ++i) {
        associativeArray = new HashMap<Text, IntWritable>();
        String term = terms[i];
        
        for (int j = ((i - distanceThreshold < 0) ? 0 : (i - distanceThreshold)); j < (i + distanceThreshold); j++) {
          Text u = new Text(terms[j]);
          if (!terms[j].equalsIgnoreCase(terms[i])) {
            if (associativeArray.containsKey(u)) {
              int val = associativeArray.get(u).get();
              associativeArray.get(u).set(val + 1);
            } else  {
              associativeArray.put(new Text(terms[j]),  new IntWritable(1));
            }
          }
        } // end of the second for loop
        
        context.write(new Text(terms[i]), associativeArray);
      } // end of then first for loop
    } //end of the map function

  }
  
  public static class WordCoourrenceReducer extends Reducer <Text, HashMap<Text, IntWritable>, Text, HashMap<Text, IntWritable>> {

    @Override
    protected void reduce(Text key,
        Iterable<HashMap<Text, IntWritable>> values, Context context)
        throws IOException, InterruptedException {
      HashMap<Text, IntWritable> associativeArray = new HashMap<Text, IntWritable>();
      
      for (HashMap<Text, IntWritable> val : values) {
       Iterator<Map.Entry<Text, IntWritable>> entryIter = (Iterator<Entry<Text, IntWritable>>) val.entrySet().iterator(); 
       while (entryIter.hasNext()) {
         Map.Entry<Text, IntWritable> entry = entryIter.next();
         if (associativeArray.containsKey(entry.getKey())) {
           associativeArray.get(entry.getKey()).set(associativeArray.get(entry.getKey()).get() + 1);
         } else {
           associativeArray.put(entry.getKey(), entry.getValue());
         }
       } // end of while
      } // end of for
      
      context.write(key, associativeArray);
    }
    
  }
  
  public int run(String[] args) throws Exception {
    Job job = getJobConf(args);
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo = new JobInfo() {

      @Override
      public Class<?> getJarByClass() {
        // TODO Auto-generated method stub
        return WordCoourrenceMatricesDriver.class;
      }

      @Override
      public Class<? extends Mapper> getMapperClass() {
        // TODO Auto-generated method stub
        return WordCoourenceMapper.class;
      }

      @Override
      public Class<? extends Reducer> getCombinerClass() {
        // TODO Auto-generated method stub
        return WordCoourrenceReducer.class;
      }

      @Override
      public Class<? extends Reducer> getReducerClass() {
        // TODO Auto-generated method stub
        return WordCoourrenceReducer.class;
      }

      @Override
      public Class<?> getOutputKeyClass() {
        // TODO Auto-generated method stub
        return Text.class;
      }

      @Override
      public Class<?> getOutputValueClass() {
        // TODO Auto-generated method stub
        return Text.class;
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
    return setupJob("Wrod Co-courrence Matrice", jobInfo);
  }
  
  public static void main(String[] args) throws Exception {
    System.out.println(ToolRunner.run(new Configuration(), new WordCoourrenceMatricesDriver(), args));
  }

}
