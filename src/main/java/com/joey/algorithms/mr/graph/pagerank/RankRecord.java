package com.joey.algorithms.mr.graph.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RankRecord implements Writable {
  private double rank;
  private String[] urlList = new String[0];

  public RankRecord() {
  }

  public double getRank() {
    return rank;
  }

  public String[] getUrlList() {
    return urlList;
  }

  public RankRecord(String str) {
    super();
    String[] items = str.split(" ");
    this.rank = Double.parseDouble(items[0]);
    if (items.length == 2)
      this.urlList = items[1].split("<;>");
  }

  public RankRecord(double rank, String[] urlList) {
    super();
    this.rank = rank;
    this.urlList = urlList;
  }

  public String toString() {
    String res = "";
    for (String s : urlList) {
      res += s + "<;>";
    }
    return rank + " " + res;
  }

  public void readFields(DataInput in) throws IOException {
    DoubleWritable dw = new DoubleWritable();
    ArrayWritable aw = new ArrayWritable(Text.class);
    dw.readFields(in);
    aw.readFields(in);
    rank = dw.get();
    urlList = aw.toStrings();
  }

  public void write(DataOutput out) throws IOException {
    DoubleWritable dw = new DoubleWritable(rank);
    ArrayWritable aw = new ArrayWritable(urlList);
    dw.write(out);
    aw.write(out);
  }
}
