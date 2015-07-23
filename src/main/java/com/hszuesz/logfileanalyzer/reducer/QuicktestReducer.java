package com.hszuesz.logfileanalyzer.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class QuicktestReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
    private final IntWritable objTotal = new IntWritable();
    
    @Override
    public void reduce(Text objKey, Iterable<IntWritable> arrValues, Context objContext) throws IOException, InterruptedException {
        int lngSum = 0;
        
        for(IntWritable objValue : arrValues) {
            lngSum += objValue.get();
        }
        
        this.objTotal.set(lngSum);
        
        objContext.write(objKey, this.objTotal);
    }
}
