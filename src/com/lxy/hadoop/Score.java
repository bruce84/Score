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
		// ʵ��map����
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// ������Ĵ��ı��ļ�������ת����String
			String line = value.toString();
			StringTokenizer tokenizerLine = new StringTokenizer(line);
			Text name = new Text();
			int scoreInt = 0;

			if (tokenizerLine.hasMoreTokens()) {
				String strName = tokenizerLine.nextToken();// ѧ����������
				name = new Text(strName);
			}
			if (tokenizerLine.hasMoreTokens()) {
				String strScore = tokenizerLine.nextToken();// �ɼ�����
				scoreInt = Integer.parseInt(strScore);
			}
			// ��������ͳɼ�
			context.write(name, new IntWritable(scoreInt));
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		// ʵ��reduce����
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			// Iterator<IntWritable> iterator = values.iterator();
			// while (iterator.hasNext()) {
			// sum += iterator.next().get();// �����ܷ�
			// count++;// ͳ���ܵĿ�Ŀ��
			// }
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			int average = (int) sum / count;// ����ƽ���ɼ�
			context.write(key, new IntWritable(average));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//��δ����ܱ�֤eclipse�ύ��ҵ��yarn; ���û����δ��룬����ύ��local��map/reduce�����ڱ���ִ�У�
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
		// ����Map��Combine��Reduce������
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// �����������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// ����������ݼ��ָ��С���ݿ�splites���ṩһ��RecordReder��ʵ��
		job.setInputFormatClass(TextInputFormat.class);
		// �ṩһ��RecordWriter��ʵ�֣������������
		job.setOutputFormatClass(TextOutputFormat.class);
		// ������������Ŀ¼
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
