package com.hszuesz.logfileanalyzer.inputformat;

import com.hszuesz.logfileanalyzer.recordreader.LinePatternRecordReader;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class LinePatternInputFormat extends FileInputFormat<Text, IntWritable> {

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new LinePatternRecordReader();
    }
    
}
