package com.joey.mapred.chainjobs;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainJobs extends Configured implements Tool {

	public static class TokenizerMapper extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
		    OutputCollector<Text, IntWritable> output, Reporter reporter)
		    throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class UppercaseMapper extends MapReduceBase implements
	    Mapper<Text, IntWritable, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text key, IntWritable value,
		    OutputCollector<Text, IntWritable> output, Reporter reporter)
		    throws IOException {
			String line = key.toString();
			word.set(line.toUpperCase());
			output.collect(word, one);
		}
	}

	public static class Reduce extends MapReduceBase implements
	    Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
		    OutputCollector<Text, IntWritable> output, Reporter reporter)
		    throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws IOException {

		Configuration conf = getConf();
		JobConf job = new JobConf(conf);
		
		job.setJarByClass(ChainJobs.class);

		job.setJobName("TestforChainJobs");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobConf map1Conf = new JobConf(false);
		ChainMapper.addMapper(job, TokenizerMapper.class, LongWritable.class, Text.class,
		    Text.class, IntWritable.class, true, map1Conf);

		JobConf map2Conf = new JobConf(false);
		ChainMapper.addMapper(job, UppercaseMapper.class, Text.class, IntWritable.class,
		    Text.class, IntWritable.class, true, map2Conf);

		JobConf reduceConf = new JobConf(false);
		ChainReducer.setReducer(job, Reduce.class, Text.class, IntWritable.class,
		    Text.class, IntWritable.class, true, reduceConf);

		JobClient.runJob(job);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ChainJobs(), args);
		System.exit(res);
	}

}
