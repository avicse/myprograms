package myprojbda;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalSalary {

public static class MapperClass extends
  Mapper<LongWritable, Text, Text, FloatWritable> {
 public void map(LongWritable key, Text empRecord, Context con) throws IOException, InterruptedException {
  String[] word = empRecord.toString().split("\\t");
 try {
   Float salary = Float.parseFloat(word[2]);
   if(salary > 0.0 & salary <= 1000)
       con.write(new Text("one"), new FloatWritable(salary));
   else if(salary > 1000 & salary <= 2000)
       con.write(new Text("two"), new FloatWritable(salary));
   else if(salary > 2000 & salary <= 3000)
       con.write(new Text("three"), new FloatWritable(salary));
   else
       con.write(new Text("others"), new FloatWritable(salary));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
}

public static class ReducerClass extends
  Reducer<Text, FloatWritable, Text, Text> {
 public void reduce(Text key, Iterable<FloatWritable> valueList, Context con) throws IOException, InterruptedException {
  try {
   Float total = (float) 0;
   int count = 0;
   for (FloatWritable var : valueList) {
    total += var.get();
    System.out.println("reducer " + var.get());
    count++;
   }
   Float avg = (Float) total / count;
   String out = "Total: " + total + " :: " + "Average: " + avg +" :: "+ "Count: " +count;
   con.write(key, new Text(out));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
}

public static void main(String[] args) {
 Configuration conf = new Configuration();
 try {
  Job job = Job.getInstance(conf, "FindSalary Groups");
  job.setJarByClass(TotalSalary.class);
  job.setMapperClass(MapperClass.class);
  job.setReducerClass(ReducerClass.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(FloatWritable.class);
  
  Path pathInput = new Path(args[0]);     //"/home/avinashp/Desktop/emprecords.txt");
  Path pathOutputDir = new Path(args[1]);           //"/home/avinashp/Desktop/empout.txt");
  FileInputFormat.addInputPath(job, pathInput);
  FileOutputFormat.setOutputPath(job, pathOutputDir);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 } 
 catch (IOException e) {
  e.printStackTrace();
 } 
 catch (ClassNotFoundException e) {
  e.printStackTrace();
 } 
 catch (InterruptedException e) {
  e.printStackTrace();
 }

}
}

