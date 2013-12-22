package com.joey.mapred.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {
               
                /*We declare three variables for Mapper Driver , Reducer Driver , MapReduceDrivers
                Generics parameters for each of them is point worth noting
                MapDriver generics matches with our test Mapper generics

                SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable>
                Similarly for ReduceDriver we have same matching generics declaration with

                SMSCDRReducer extends Reducer<Text, IntWritable, Text, IntWritable>*/
               
   MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
   MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
   ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

  
   //create instances of our Mapper , Reducer .
   //Set the corresponding mappers and reducers using setXXX() methods
   @Before
   public void setUp() {
      WordCount.WordCountMapper mapper = new WordCount.WordCountMapper();
      WordCount.WordCountReducer reducer = new WordCount.WordCountReducer();
      mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
      mapDriver.setMapper(mapper);
      reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
      reduceDriver.setReducer(reducer);
      mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
      mapReduceDriver.setMapper(mapper);
      mapReduceDriver.setReducer(reducer);
   }

   @Test
   public void testMapper() throws IOException {
                   //gave one sample line input to the mapper
      mapDriver.withInput(new LongWritable(1), new Text("sky sky sky oh my beautiful sky"));
      //expected output for the mapper
      mapDriver.withOutput(new Text("sky"), new LongWritable(1));
      mapDriver.withOutput(new Text("sky"), new LongWritable(1));
      mapDriver.withOutput(new Text("sky"), new LongWritable(1));
      mapDriver.withOutput(new Text("oh"), new LongWritable(1));
      mapDriver.withOutput(new Text("my"), new LongWritable(1));
      mapDriver.withOutput(new Text("beautiful"), new LongWritable(1));
      mapDriver.withOutput(new Text("sky"), new LongWritable(1));
      //runTest() method run the Mapper test with input
      mapDriver.runTest();
   }

   @Test
   public void testReducer() throws IOException {
      List<LongWritable> values = new ArrayList<LongWritable>();
      values.add(new LongWritable(1));
      values.add(new LongWritable(1));
      reduceDriver.withInput(new Text("sky"), values);
      reduceDriver.withOutput(new Text("sky"), new LongWritable(2));
      reduceDriver.runTest();
   }

 @Test
   public void testMapReduce() throws IOException {
      mapReduceDriver.withInput(new LongWritable(1), new Text("sky sky sky"));
      mapReduceDriver.addOutput(new Text("sky"), new LongWritable(3));
   
      mapReduceDriver.runTest();
   }
}


