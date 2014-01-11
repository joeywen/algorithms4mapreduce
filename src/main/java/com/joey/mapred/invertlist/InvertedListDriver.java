package com.joey.mapred.invertlist;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.joey.mapred.BaseDriver;
import com.joey.mapred.utils.TreeMapIIWritable;

public class InvertedListDriver extends BaseDriver {

  /**
   * input format
   *    docid<tab>doc content
   *    
   * output format
   *    (term:docid)<tab>(tf in this doc)
   *
   */
  public static class InvertedListMap extends Mapper<IntWritable/*docid*/, Text/*doc content*/, Text, IntWritable> {

    @Override
    protected void map(IntWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      HashMap<Text, IntWritable> freqs = new HashMap<Text, IntWritable> ();
      
      // the document can be preprocessed by thrid analynized tool first
      // here is simplify this procedure using split by whitespace instead
      String[] terms = value.toString().split(" "); 
      for (String term : terms ) {
        
        if (term == null || "".equals(term)) continue;
        
        if (freqs.containsKey(new Text(term))) {
          int tf = freqs.get(new Text(term)).get();
          freqs.put(new Text(term), new IntWritable(tf + 1));
        } else {
          freqs.put(new Text(term), new IntWritable(1));
        }
      } // end of for loop
      
      Iterator<Map.Entry<Text, IntWritable>> entryIter = (Iterator<Entry<Text, IntWritable>>) freqs.entrySet().iterator();
      while (entryIter.hasNext()) {
        Map.Entry<Text, IntWritable> entry = entryIter.next();
        Text tuple = new Text();
        tuple.set(entry.getKey().toString() + ":" + key.toString());
        context.write(tuple, freqs.get(entry.getKey()));
      }
    }
    
  }
  
  public static class InvertedListReduce extends Reducer <Text, IntWritable, Text, TreeMapIIWritable> {

    private String term = null;
    private TreeMapIIWritable posting = null;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      term = null;
      
      // use TreeMap to sort the element automaticly
      posting = new TreeMapIIWritable();
    }

    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      context.write(new Text(term), posting);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      String[] tuple = key.toString().split(":");
      if (term != null && !term.equalsIgnoreCase(tuple[0])) {
        context.write(new Text(tuple[0]), posting);
        posting.clear();
      } else {
        for (IntWritable val : values) {
          posting.put(new IntWritable(Integer.parseInt(tuple[1])), val);
        }
        
        term = key.toString();
      }
    }
    
  }
  
  public int run(String[] arg0) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  protected Job getJobConf(String[] args) throws Exception {
    JobInfo jobInfo  = new JobInfo() {

		@Override
		public Class<?> getJarByClass() {
			// TODO Auto-generated method stub
			return InvertedListDriver.class;
		}

		@Override
		public Class<? extends Mapper> getMapperClass() {
			// TODO Auto-generated method stub
			return InvertedListMap.class;
		}

		@Override
		public Class<? extends Reducer> getCombinerClass() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<? extends Reducer> getReducerClass() {
			// TODO Auto-generated method stub
			return InvertedListReduce.class;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			// TODO Auto-generated method stub
			return Text.class;
		}

		@Override
		public Class<?> getOutputValueClass() {
			// TODO Auto-generated method stub
			return TreeMapIIWritable.class;
		}

		@Override
		public Class<? extends InputFormat> getInputFormatClass() {
			return TextInputFormat.class;
		}

		@Override
		public Class<? extends OutputFormat> getOutputFormatClass() {
			// TODO Auto-generated method stub
			return null;
		}
    	
    };
    return super.setupJob("Inverted List", jobInfo);
  }

}

