package com.hszuesz.logfileanalyzer.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Extending {@link Reducer} for individual use.
 *
 * Individual extension to {@link Reducer} for creating a count of some sort.
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
    private final IntWritable objTotal = new IntWritable();

    /**
     * Sum up all values for one key and write new sum to corresponding key
     *
     * @param objKey Current key
     * @param arrValues Array of values for the passed key
     * @param objContext Current context
     * @throws IOException
     * @throws InterruptedException
     */
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
