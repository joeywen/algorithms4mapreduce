package com.joey.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public abstract class BaseDriver extends Configured implements Tool {

  // method to set the configuration for the job and 
  // the mapper and the reducer classes
  protected Job setupJob(String jobName, JobInfo jobInfo) throws Exception {
  	Configuration conf = getConf();
  	
  	if (conf == null) {
  		throw new RuntimeException("Configuration should not be null");
  	}
  	
    Job job = new Job(conf, jobName);
    // set the several classes
    job.setJarByClass(jobInfo.getJarByClass());
    // set the mapper class
    job.setMapperClass(jobInfo.getMapperClass());

    // the combiner class is optional, so set it only if it
    // is required by the program
    if (jobInfo.getCombinerClass() != null)
      job.setCombinerClass(jobInfo.getCombinerClass());

    // set the reducer class
    job.setReducerClass(jobInfo.getReducerClass());

    // the number of reducers is set to 3, this can be
    // altered according to the program's requirements
    job.setNumReduceTasks(3);

    // set the type of the output key and value for the
    // Map & Reduce functions
    job.setOutputKeyClass(jobInfo.getOutputKeyClass());
    job.setOutputValueClass(jobInfo.getOutputValueClass());
    if (jobInfo.getInputFormatClass() != null)
      job.setInputFormatClass(jobInfo.getInputFormatClass());
    if (jobInfo.getOutputFormatClass() != null)
      job.setOutputFormatClass(jobInfo.getOutputFormatClass());
    return job;
  }

  protected abstract Job getJobConf(String[] args) throws Exception;
  
  protected abstract class JobInfo {
    public abstract Class<?> getJarByClass();

    public abstract Class<? extends Mapper> getMapperClass();

    public abstract Class<? extends Reducer> getCombinerClass();

    public abstract Class<? extends Reducer> getReducerClass();

    public abstract Class<?> getOutputKeyClass();

    public abstract Class<?> getOutputValueClass();
    
    public abstract Class<? extends InputFormat> getInputFormatClass();
    
    public abstract Class<? extends OutputFormat> getOutputFormatClass();
  }
}
