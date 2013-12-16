package com.joey.algorithms.mr.matrix.onestep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import com.joey.algorithms.mr.graph.BaseDriver;

/**
 *  matrix A         matrix B
 *  | 0 1 2 3 4 |   | 0  1  2 |
 *  | 5 6 7 8 9 |   | 3  4  5 |
 *                  | 6  7  8 |
 *                  | 9 10 11 |
 *                  | 12 13 14|
 *                  
 * want to matrix A multiple matrix B
 * 
 * @author Joey
 *
 * reference by http://importantfish.com/one-step-matrix-multiplication-with-hadoop/
 */

public class OneStepMatrixMultiplicationDriver extends BaseDriver {

	public int run(String[] arg0) throws Exception {
		conf.set("m", "2");
		conf.set("n", "5");
		conf.set("p", "3");
		
		Job job = getJobConf(arg0);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	protected Job getJobConf(String[] args) throws Exception {
		JobInfo jobInfo = new JobInfo(){

			@Override
			public Class<?> getJarByClass() {
				// TODO Auto-generated method stub
				return OneStepMatrixMultiplicationDriver.class;
			}

			@Override
			public Class<? extends Mapper> getMapperClass() {
				// TODO Auto-generated method stub
				return OneStepMapper.class;
			}

			@Override
			public Class<? extends Reducer> getCombinerClass() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Class<? extends Reducer> getReducerClass() {
				// TODO Auto-generated method stub
				return OneStepReducer.class;
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
				return TextInputFormat.class;
			}

			@Override
			public Class<? extends OutputFormat> getOutputFormatClass() {
				// TODO Auto-generated method stub
				return TextOutputFormat.class;
			}
			
		};
		return setupJob("one-step-matrix-multi", jobInfo);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new OneStepMatrixMultiplicationDriver(), args));
	}
}
