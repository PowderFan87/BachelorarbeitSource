package com.hszuesz.logfileanalyzer.mapper;

import com.hszuesz.logfileanalyzer.Main;
import com.hszuesz.logfileanalyzer.Quicktest;
import java.io.IOException;
import java.util.logging.Level;
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
public class PatternMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text objTmpKey = new Text();
    
    @Override
    public void map(Object objKey, Text objValue, Mapper.Context objContext) throws IOException, InterruptedException {
        Configuration   objConf             = objContext.getConfiguration();
        Pattern         objKeyPattern       = Pattern.compile(objConf.get("lfa.mapper.pattern.key"));
        String          strNoMatchAction    = objConf.get("lfa.mapper.nomatchaction");
        
        String  strLine = objValue.toString();
        
        Matcher objMatcher  = objKeyPattern.matcher(strLine);
        
        if (objMatcher.find()) {
            objTmpKey.set(objMatcher.group(1));
        
            objContext.write(objTmpKey, Quicktest.objOne);
        } else {
            if ("SKIP".equals(strNoMatchAction)) {
                Main.objLogger.log(Level.WARNING, "Can't find matching key for pattern {0}. SKIP", objConf.get("lfa.mapper.pattern.key"));
            } else {
                Main.objLogger.log(Level.WARNING, "Can't find matching key. Using NA");

                objTmpKey.set("NA");
        
                objContext.write(objTmpKey, Quicktest.objOne);
            }
        }
    }
}
