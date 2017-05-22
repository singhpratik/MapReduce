/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package delayreasonbypercentage;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class DelayReasonByPercentage {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
       Configuration conf  = new Configuration();
            Job job  = Job.getInstance(conf, "Average ");
            job.setJarByClass(DelayPercentage_CompositeKey.class);
            job.setMapperClass(Mapper_DelayPercentage.class);
            job.setMapOutputKeyClass(IntWritable.class);
           job.setMapOutputValueClass(DelayPercentage_CompositeKey.class);
            // job.setGroupingComparatorClass(AverageDelay_GroupComparator.class);
           // job.setCombinerClass(AverageDelay_Reducer.class);
            job.setReducerClass(Reducer_DelayByPercentage.class);
           // job.setNumReduceTasks(3);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DelayPercentage_CompositeKey.class);
           // job.setPartitionerClass(AverageDelay_Partitioner.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)? 1:0);
    }
    
}
