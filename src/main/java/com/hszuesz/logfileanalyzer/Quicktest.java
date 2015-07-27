package com.hszuesz.logfileanalyzer;

import com.hszuesz.logfileanalyzer.mapper.QuicktestMapper;
import com.hszuesz.logfileanalyzer.reducer.QuicktestReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class Quicktest extends Configured implements Tool{
    public final static IntWritable objOne = new IntWritable(1);
    
    @Override
    public int run(String[] arrArguments) throws Exception {
        Configuration objConf = this.getConf();
        
        objConf.set("logfileanalyzer.analyzer.keypattern", "\\] ([A-Z]+) \\[");
        
        Job objJob = new Job(objConf, "Count Error Types in Log");
        
        objJob.setJarByClass(Quicktest.class);
        objJob.setMapperClass(QuicktestMapper.class);
        objJob.setReducerClass(QuicktestReducer.class);
        objJob.setOutputKeyClass(Text.class);
        objJob.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(objJob, new Path(arrArguments[0]));
        objJob.setInputFormatClass(TextInputFormat.class);
        
        FileOutputFormat.setOutputPath(objJob, new Path(arrArguments[1]));
        objJob.setOutputFormatClass(TextOutputFormat.class);
        
        return objJob.waitForCompletion(true) ? 0 : 1;
    }
    
}
