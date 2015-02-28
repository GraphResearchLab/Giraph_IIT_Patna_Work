package iitp.hadoop.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class EdgeToAdjList {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(value.toString().charAt(0)!='#')
			{
			String[] line = value.toString().split("\t");

			String source, dest;

			source = line[0];
			dest = line[1];
			context.write(new Text(source), new Text(dest));
			context.write(new Text(dest), new Text(source));
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

			String output = "";
			HashSet<String> set = new HashSet<String>();
			String temp;
			for (Text string : value) {
				set.add(string.toString());
			}
			for (String string : set) {

				output = output + string + ",";
			}
			output = output.substring(0, output.length() - 1);
			context.write(key, new Text(output));

		}
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "EdgeToAdjList");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		File directory = new File("/home/nishant/output");
		FileUtils.deleteDirectory(directory);
		
		FileInputFormat.setInputPaths(job, new Path(
				"/home/nishant/inputGraph/EdgeList/amazon"));
		FileOutputFormat.setOutputPath(job, new Path("/home/nishant/output"));

		job.waitForCompletion(true);
	}

}
