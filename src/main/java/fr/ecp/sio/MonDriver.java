package fr.ecp.sio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by olivierpasquier on 26/02/16.
 */
public class MonDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new MonDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        //create a job, which is kind of the map reduce client
        final Job job = Job.getInstance(getConf(),"sample-job");

        job.setJarByClass(MonDriver.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AverageDayTempMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);

        job.submit();
        return job.waitForCompletion(true) ? 0 : 1;
    }
}