package com.hszuesz.logfileanalyzer.reducer;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Extending {@link Reducer} for individual use.
 *
 * Individual extension to {@link Reducer} for calculating the average value
 * for a given key
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class AverageReducer extends Reducer<Text, IntWritable, Text, FloatWritable>  {
    private final FloatWritable objAverage = new FloatWritable();

    /**
     * Calculate the average from all values for a given key
     *
     * @param objKey Current key
     * @param arrValues Array of values for the passed key
     * @param objContext Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text objKey, Iterable<IntWritable> arrValues, Context objContext) throws IOException, InterruptedException {
        float   fltAvg      = (float) 0.0;
        int     lngCount    = 0;

        for(IntWritable objValue : arrValues) {
            fltAvg += objValue.get();
            lngCount++;
        }
        
        fltAvg /= lngCount;

        this.objAverage.set(fltAvg);

        objContext.write(objKey, this.objAverage);
    }
}
