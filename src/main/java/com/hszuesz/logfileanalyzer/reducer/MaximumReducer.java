package com.hszuesz.logfileanalyzer.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Extending {@link Reducer} for individual use.
 *
 * Individual extension to {@link Reducer} to get the maximum value for a
 * given key
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class MaximumReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
    private final IntWritable objMaximum = new IntWritable();

    /**
     * Find the maximum value for a given key
     *
     * @param objKey Current key
     * @param arrValues Array of values for the passed key
     * @param objContext Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text objKey, Iterable<IntWritable> arrValues, Context objContext) throws IOException, InterruptedException {
        int lngMax = Integer.MIN_VALUE;
        
        for(IntWritable objValue : arrValues) {
            if (objValue.get() > lngMax) {
                lngMax = objValue.get();
            }
        }
        
        this.objMaximum.set(lngMax);

        objContext.write(objKey, this.objMaximum);
    }
}
