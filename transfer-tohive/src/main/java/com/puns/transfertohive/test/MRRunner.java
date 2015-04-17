package com.puns.transfertohive.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.puns.transfertohive.NutchDumpInputFormat;

public class MRRunner {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		// for test
		System.out.println("----------BEGIN-----------");

		// String[] args = new String[2];
		// String workPath = TestCommon.BASE_INPUT_FOLDER +
		// "zamazonSellerinfo/";
		// args[0] = workPath + "itemcount.txt";
		// args[1] = TestCommon.BASE_OUTPUT_FOLDER + "itemcount/";
		// FileUtils.deleteDirectory(new File(args[1]));

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MulOutput");

//		String[] remainingArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
//
//		if (remainingArgs.length != 2) {
//			System.err.println("Error!");
//			System.exit(1);
//		}
		Path in = new Path(
				"/home/zjx/development/workspace/transfer-tohive/test/input");
		Path out = new Path(
				"/home/zjx/development/workspace/transfer-tohive/test/output"
						+ System.currentTimeMillis());

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// 推测执行设置为false，但是不了解map的和全局有什么关系
		// TODO
		job.setSpeculativeExecution(false);
		job.setMapSpeculativeExecution(false);

//		job.setJarByClass(MRRunner.class);
//		job.setMapperClass(TestMapper.class);
//		job.setInputFormatClass(NutchDumpInputFormat.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NullWritable.class);
//		job.setNumReduceTasks(0);

		// 正常他退出
		job.waitForCompletion(true);

	}

}
