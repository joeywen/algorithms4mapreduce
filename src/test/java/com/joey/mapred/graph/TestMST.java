package com.joey.mapred.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestMST {
  
  MapReduceDriver<Object, Text, IntWritable, Text, Text, Text> mapReduceDriver = null;
  MapDriver<Object, Text, IntWritable, Text> mapDriver = null;
  ReduceDriver<IntWritable, Text, Text, Text> reduceDriver = null;
  
  @Before
  public void setup() {
    mapDriver = new MapDriver<Object, Text, IntWritable, Text>();
    reduceDriver = new ReduceDriver<IntWritable, Text, Text, Text>();
    
    MSTDriver.MSTMapper mapper = new MSTDriver.MSTMapper();
    MSTDriver.MSTReducer reducer = new MSTDriver.MSTReducer();
    
    mapDriver.setMapper(mapper);
    reduceDriver.setReducer(reducer);
    
    mapReduceDriver = new MapReduceDriver(mapper, reducer);
  }
  
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new IntWritable(), new Text("6 A C"));
    mapDriver.withInput(new IntWritable(), new Text("5 C E"));
    
    mapDriver.withOutput(new IntWritable(6), new Text("A:C"));
    mapDriver.withOutput(new IntWritable(5), new Text("C:E"));
    
    mapDriver.runTest();
  }
  
  @Test
  public void testReducer() throws IOException {
    List<Text> value1 = new ArrayList<Text>();
    List<Text> value2 = new ArrayList<Text>();
    List<Text> value3 = new ArrayList<Text>();
    List<Text> value4 = new ArrayList<Text>();
    List<Text> value5 = new ArrayList<Text>();
    List<Text> value6 = new ArrayList<Text>();
    List<Text> value7 = new ArrayList<Text>();
    List<Text> value8 = new ArrayList<Text>();
    List<Text> value9 = new ArrayList<Text>();
    List<Text> value10 = new ArrayList<Text>();
    
    value1.add(new Text("A:C"));
    value2.add(new Text("C:E"));
    value3.add(new Text("C:F"));
    value4.add(new Text("C:D"));
    value5.add(new Text("B:C"));
    value6.add(new Text("B:D"));
    value7.add(new Text("A:D"));
    value8.add(new Text("E:F"));
    value9.add(new Text("D:F"));
    value10.add(new Text("A:B"));
    
    // key/value from map send to reduce, and all of these key/value are 
    // sort by the key ascending order. So the following output should be 
    // follow that rules.
    reduceDriver.withInput(new IntWritable(1), value5);
    reduceDriver.withInput(new IntWritable(2), value4);
    reduceDriver.withInput(new IntWritable(2), value6);
    reduceDriver.withInput(new IntWritable(3), value3);
    reduceDriver.withInput(new IntWritable(4), value7);
    reduceDriver.withInput(new IntWritable(4), value8);
    reduceDriver.withInput(new IntWritable(4), value9);
    reduceDriver.withInput(new IntWritable(5), value2);
    reduceDriver.withInput(new IntWritable(5), value10);
    reduceDriver.withInput(new IntWritable(6), value1);
    
    // the right result is result from the above input order.
    reduceDriver.withOutput(new Text("1"), new Text("B:C"));
    reduceDriver.withOutput(new Text("2"), new Text("C:D"));
    reduceDriver.withOutput(new Text("3"), new Text("C:F"));
    reduceDriver.withOutput(new Text("4"), new Text("A:D"));
    reduceDriver.withOutput(new Text("4"), new Text("E:F"));
    
    reduceDriver.runTest();
  }
  
  @Test
  public void testMapReducer() throws IOException {
    // input
    mapReduceDriver.withInput(new IntWritable(), new Text("6 A C"));
    mapReduceDriver.withInput(new IntWritable(), new Text("5 C E"));
    mapReduceDriver.withInput(new IntWritable(), new Text("3 C F"));
    mapReduceDriver.withInput(new IntWritable(), new Text("2 C D"));
    mapReduceDriver.withInput(new IntWritable(), new Text("1 B C"));
    mapReduceDriver.withInput(new IntWritable(), new Text("2 B D"));
    mapReduceDriver.withInput(new IntWritable(), new Text("4 A D"));
    mapReduceDriver.withInput(new IntWritable(), new Text("4 E F"));
    mapReduceDriver.withInput(new IntWritable(), new Text("4 D F"));
    mapReduceDriver.withInput(new IntWritable(), new Text("5 A B"));
    // output
    mapReduceDriver.withOutput(new Text("1"), new Text("B:C"));
    mapReduceDriver.withOutput(new Text("2"), new Text("C:D"));
    mapReduceDriver.withOutput(new Text("3"), new Text("C:F"));
    mapReduceDriver.withOutput(new Text("4"), new Text("A:D"));
    mapReduceDriver.withOutput(new Text("4"), new Text("E:F"));
    
    mapReduceDriver.runTest();
  }
  
}
