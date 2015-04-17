package com.puns.transfertohive;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.base.Charsets;

public class NutchDumpInputFormat extends
		FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		String delimiter = "Recno::";
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);

		return new LineRecordReader(job, (FileSplit) split,
				recordDelimiterBytes);
	}
}
