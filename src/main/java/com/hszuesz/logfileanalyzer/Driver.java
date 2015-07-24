package com.hszuesz.logfileanalyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 * @param <K>
 * @param <V>
 */
public class Driver<K, V> extends Configured implements Tool {
    private Class<? extends Mapper>         clsMapperClass;
    private Class<? extends Reducer>        clsReducerClass;
    private Class<? extends K>              clsOutputKeyClass;
    private Class<? extends V>              clsOutputValueClass;
    private Class<? extends InputFormat>    clsInputFormatClass;
    private Class<? extends OutputFormat>   clsOutputFormatClass;
    private String                          strJobName;
    
    public  V                               objDefaultValue;
    
    @Override
    public int run(String[] arrArguments) throws Exception {
        Configuration objConf = this.getConf();
        
        Job objJob = new Job(objConf, this.strJobName);
        
        objJob.setJarByClass(this.getClass());
        objJob.setMapperClass(this.clsMapperClass);
        objJob.setReducerClass(this.clsReducerClass);
        objJob.setOutputKeyClass(this.clsOutputKeyClass);
        objJob.setOutputValueClass(this.clsOutputValueClass);
        
        FileInputFormat.addInputPath(objJob, new Path(arrArguments[0]));
        objJob.setInputFormatClass(this.clsInputFormatClass);
        
        FileOutputFormat.setOutputPath(objJob, new Path(arrArguments[1]));
        objJob.setOutputFormatClass(this.clsOutputFormatClass);
        
        return objJob.waitForCompletion(true) ? 0 : 1;
    }

    public void setClsInputFormatClass(Class<? extends InputFormat> clsInputFormatClass) {
        this.clsInputFormatClass = clsInputFormatClass;
    }

    public void setClsOutputFormatClass(Class<? extends OutputFormat> clsOutputFormatClass) {
        this.clsOutputFormatClass = clsOutputFormatClass;
    }

    public void setClsMapperClass(Class<? extends Mapper> clsMapperClass) {
        this.clsMapperClass = clsMapperClass;
    }

    public void setClsReducerClass(Class<? extends Reducer> clsReducerClass) {
        this.clsReducerClass = clsReducerClass;
    }

    public void setClsOutputKeyClass(Class<? extends K> clsOutputKeyClass) {
        this.clsOutputKeyClass = clsOutputKeyClass;
    }

    public void setClsOutputValueClass(Class<? extends V> clsOutputValueClass) {
        this.clsOutputValueClass = clsOutputValueClass;
    }

    public void setObjDefaultValue(V objDefaultValue) {
        this.objDefaultValue = objDefaultValue;
    }

    public void setStrJobName(String strJobName) {
        this.strJobName = strJobName;
    }
    
    
}
