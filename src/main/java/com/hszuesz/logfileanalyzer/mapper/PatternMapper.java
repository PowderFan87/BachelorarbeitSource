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
 * Extention for Hadoop class {@link Mapper} to create key by a pattern
 * 
 * Individual Mapper class to create key value pairs by searching for a key
 * via a regular expression. Configuration via properties file needed.
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class PatternMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text objTmpKey = new Text();
    
    /**
     * Map methode called by hadoop core.
     * 
     * Mapping the found data to a new key value pari by extracting the key from
     * the passed value by use of a regular expression defined in the properties.
     * If the pattern isn't a match the entry is added with the key NA or skiped
     * depending on the configuration.
     * 
     * @param objKey Object for current key
     * @param objValue Object for current value.
     * @param objContext Current context for this mapper
     * @throws IOException
     * @throws InterruptedException 
     */
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
                Logger.getLogger(PatternMapper.class.getName()).log(Level.WARNING, "Can't find matching key for pattern {0}. SKIP", objConf.get("lfa.mapper.pattern.key"));
            } else {
                Logger.getLogger(PatternMapper.class.getName()).log(Level.WARNING, "Can't find matching key. Using NA");

                objTmpKey.set("NA");
        
                objContext.write(objTmpKey, Quicktest.objOne);
            }
        }
    }
}
