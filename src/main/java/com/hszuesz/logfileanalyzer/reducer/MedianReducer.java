package com.hszuesz.logfileanalyzer.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Extending {@link Reducer} for individual use.
 *
 * Individual extension to {@link Reducer} to get the median for a given key
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class MedianReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
    private final IntWritable objMedian = new IntWritable();

    /**
     * Find the median for a given key
     *
     * @param objKey Current key
     * @param arrValues Array of values for the passed key
     * @param objContext Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text objKey, Iterable<IntWritable> arrValues, Context objContext) throws IOException, InterruptedException {
        int                 lngMedian;
        int                 lngCount    = 0;
        ArrayList<Integer>  lstValues   = new ArrayList<>();
        
        for(IntWritable objValue : arrValues) {
            lstValues.add(objValue.get());
            lngCount++;
        }
        
        Collections.sort(lstValues);
        
        if (lngCount % 2 == 0) {
            lngMedian = (lstValues.get(lngCount / 2) + lstValues.get((lngCount / 2) + 1)) / 2;
        } else {
            lngMedian = lstValues.get((lngCount + 1) / 2);
        }
        
        this.objMedian.set(lngMedian);

        objContext.write(objKey, this.objMedian);
    }
}
