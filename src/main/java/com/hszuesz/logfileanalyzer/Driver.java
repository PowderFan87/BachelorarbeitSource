package com.hszuesz.logfileanalyzer;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import org.apache.hadoop.util.ToolRunner;

/**
 * {@link Driver} for execution of MapReduce job via {@link ToolRunner} class
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 * @param <K> Class for type of key
 * @param <V> Class for type of value
 */
public class Driver<K, V> extends Configured implements Tool {
    private final HashMap<String, String>   mapAdditionalConfigurations = new HashMap<>();
    
    private Class<? extends Mapper>         clsMapperClass;
    private Class<? extends Reducer>        clsReducerClass;
    private Class<? extends K>              clsOutputKeyClass;
    private Class<? extends V>              clsOutputValueClass;
    private Class<? extends InputFormat>    clsInputFormatClass;
    private Class<? extends OutputFormat>   clsOutputFormatClass;
    private String                          strJobName;
    
    public  V                               objDefaultValue;
    
    /**
     * Called by {@link ToolRunner} for execution of MapReduce job.
     * 
     * Called by {@link ToolRunner} to execute a configured MapReduce job.
     * The {@link Configuration} instance of the driver is retrieved and (if there are
     * additional configurations in HashMap) extended with additional values.
     * Next a new Job instance is created with the configuration. All relevant
     * classes are set and then the job is executed (waitForCompletion).
     * The return value is an integer 0 when the job was successfull and 1 on
     * if there was an error of some kind (see logs).
     * 
     * @param arrArguments rray of two string Arguments, first being the input and secound the output directory
     * @return exit code
     * @throws Exception 
     */
    @Override
    public int run(String[] arrArguments) throws Exception {
        Configuration objConf = this.getConf();
        
        if (this.mapAdditionalConfigurations.size() > 0) {
            for (String strKey : this.mapAdditionalConfigurations.keySet()) {
                objConf.set(strKey, this.mapAdditionalConfigurations.get(strKey));
            }
        }
        
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

    /**
     * Set class for {@link InputFormat}
     * 
     * @param clsInputFormatClass Class to use for input format
     */
    public void setClsInputFormatClass(Class<? extends InputFormat> clsInputFormatClass) {
        this.clsInputFormatClass = clsInputFormatClass;
    }

    /**
     * Set class for {@link OutputFormat}
     * 
     * @param clsOutputFormatClass Class to use for output format
     */
    public void setClsOutputFormatClass(Class<? extends OutputFormat> clsOutputFormatClass) {
        this.clsOutputFormatClass = clsOutputFormatClass;
    }

    /**
     * Set class for {@link Mapper}
     * 
     * @param clsMapperClass Class to use as mapper
     */
    public void setClsMapperClass(Class<? extends Mapper> clsMapperClass) {
        this.clsMapperClass = clsMapperClass;
    }

    /**
     * Set class for {@link Reducer}
     * 
     * @param clsReducerClass Class to use as reducer
     */
    public void setClsReducerClass(Class<? extends Reducer> clsReducerClass) {
        this.clsReducerClass = clsReducerClass;
    }

    /**
     * Set class for key type in output
     * 
     * @param clsOutputKeyClass Class to use as key output type
     */
    public void setClsOutputKeyClass(Class<? extends K> clsOutputKeyClass) {
        this.clsOutputKeyClass = clsOutputKeyClass;
    }

    /**
     * Set class for value type in output
     * 
     * @param clsOutputValueClass Class to use as value output type
     */
    public void setClsOutputValueClass(Class<? extends V> clsOutputValueClass) {
        this.clsOutputValueClass = clsOutputValueClass;
    }

    /**
     * Set object for default value
     * 
     * @param objDefaultValue Object to use as default value
     */
    public void setObjDefaultValue(V objDefaultValue) {
        this.objDefaultValue = objDefaultValue;
    }

    /**
     * Set name for MapReduce job
     * 
     * @param strJobName Name of the MapReduce job
     */
    public void setStrJobName(String strJobName) {
        this.strJobName = strJobName;
    }
    
    /**
     * Add additional configuration key-value pair for job execution
     * 
     * @param strKey Key name for additional configuration
     * @param strValue Value for additional configuration
     */
    public void setAdditionalConfiguration(String strKey, String strValue) {
        Logger.getLogger(Driver.class.getName()).log(Level.INFO, "Setting additional configuration with key {0} and value {1}", new Object[]{strKey, strValue});
        
        this.mapAdditionalConfigurations.put(strKey, strValue);
    }
}
