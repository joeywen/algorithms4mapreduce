package com.joey.mapred.matrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestBlockMatrixMultiplication {

  private MapDriver<LongWritable, Text, Text, Text> mapDriver = null;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver = null;
  private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver = null;
  private BlockMatrixMultiplication blockMRDriver = null;
  
  @Before
  public void setup() {
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
    
    BlockMatrixMultiplication.Map mapper = new BlockMatrixMultiplication.Map();
    BlockMatrixMultiplication.Reduce reducer = new BlockMatrixMultiplication.Reduce();
    
    mapDriver.setMapper(mapper);
    reduceDriver.setReducer(reducer);
    
    mrDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>(mapper, reducer);
    
  }
  
  @Test
  public void testMapper() {
    
  }
  
  @Test
  public void testReducer() {
    
  }
  
  @Test
  public void testMapReduce() {
    
  }
}
