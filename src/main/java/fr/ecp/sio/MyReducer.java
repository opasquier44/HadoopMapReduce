package fr.ecp.sio;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by olivierpasquier on 03/03/16.
 */
public class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        long numberOfOccurences = 0L;
        float sum = 0f;
        for (FloatWritable value:values){
            sum += value.get();
            numberOfOccurences++;
        }

        Float average = sum / numberOfOccurences;
        context.write(key, new FloatWritable(average));
    }
}