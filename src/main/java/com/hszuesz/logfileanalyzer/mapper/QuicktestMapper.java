package com.hszuesz.logfileanalyzer.mapper;

import com.hszuesz.logfileanalyzer.Quicktest;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class QuicktestMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text objErrorType = new Text();
    
    @Override
    public void map(Object objKey, Text objValue, Context objContext) throws IOException, InterruptedException {
        Configuration   objConf         = objContext.getConfiguration();
        Pattern         objKeyPattern   = Pattern.compile("\\] ([A-Z]{4,5}) ");
        
        String  strLine = objValue.toString();
        
        Matcher objMatcher  = objKeyPattern.matcher(strLine);
        
        if (objMatcher.find()) {
            objErrorType.set(objMatcher.group(1));
        } else {
            Logger.getLogger(QuicktestMapper.class.getName()).log(Level.WARNING, "Can't find matching key. Using MISC");
            
            objErrorType.set("MISC");
        }
        
        objContext.write(objErrorType, Quicktest.objOne);
    }
}
