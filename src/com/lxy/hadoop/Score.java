package com.lxy.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Score {
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		// 实现map函数
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 将输入的纯文本文件的数据转化成String
			String line = value.toString();
			StringTokenizer tokenizerLine = new StringTokenizer(line);
			Text name = new Text();
			int scoreInt = 0;

			if (tokenizerLine.hasMoreTokens()) {
				String strName = tokenizerLine.nextToken();// 学生姓名部分
				name = new Text(strName);
			}
			if (tokenizerLine.hasMoreTokens()) {
				String strScore = tokenizerLine.nextToken();// 成绩部分
				scoreInt = Integer.parseInt(strScore);
			}
			// 输出姓名和成绩
			context.write(name, new IntWritable(scoreInt));
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		// 实现reduce函数
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			// Iterator<IntWritable> iterator = values.iterator();
			// while (iterator.hasNext()) {
			// sum += iterator.next().get();// 计算总分
			// count++;// 统计总的科目数
			// }
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			int average = (int) sum / count;// 计算平均成绩
			context.write(key, new IntWritable(average));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//这段代码能保证eclipse提交作业到yarn; 如果没有这段代码，则会提交到local（map/reduce任务在本地执行）
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.address", "192.168.241.201:8032"); 
	    conf.set("yarn.resourcemanager.scheduler.address", "192.168.241.201:8030"); 
	    conf.set("mapreduce.jobhistory.address", "192.168.241.201:10020"); 
		conf.set("mapreduce.app-submission.cross-platform", "true"); 	
		
		String[] ioArgs = new String[] { "hdfs://192.168.241.201:9000/usr/hadoop/score/score_in",
				"hdfs://192.168.241.201:9000/usr/hadoop/score/score_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Score Average <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Score Average");
		job.setJarByClass(Score.class);
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
		job.setInputFormatClass(TextInputFormat.class);
		// 提供一个RecordWriter的实现，负责数据输出
		job.setOutputFormatClass(TextOutputFormat.class);
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
