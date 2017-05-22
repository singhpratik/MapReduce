/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagedelaybymonthbyyear;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author pratik
 */
public class AverageDelayByMonthByYear {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
            Configuration conf  = new Configuration();
            Job job  = Job.getInstance(conf, "Average Delay in Each Month by Year");
            job.setJarByClass(AverageDelayByMonthByYear.class);
            job.setMapperClass(Mapper_AverageDelay.class);
            job.setMapOutputKeyClass(CompositeKey_AverageDelay.class);
            job.setMapOutputValueClass(CompositeValue_AverageDelay.class);
            // job.setGroupingComparatorClass(AverageDelay_GroupComparator.class);
           // job.setCombinerClass(AverageDelay_Reducer.class);
            job.setReducerClass(Reducer_AverageDelay.class);
           // job.setNumReduceTasks(3);
            job.setOutputKeyClass(CompositeKey_AverageDelay.class);
            job.setOutputValueClass(CompositeValue_AverageDelay.class);
           // job.setPartitionerClass(AverageDelay_Partitioner.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            boolean jobComplete =job.waitForCompletion(true);
         
          if(jobComplete){     
                Job job2  = Job.getInstance(conf, "Distribution of files by Year");
                job2.setJarByClass(AverageDelayByMonthByYear.class);

                job2.setMapperClass(Mapper_Bins.class);
                MultipleOutputs.addNamedOutput(job2, "bins", TextOutputFormat.class,Text.class, NullWritable.class);
                MultipleOutputs.setCountersEnabled(job2, true);
                job2.setNumReduceTasks(0);
                FileInputFormat.addInputPath(job2, new Path(args[1]));
                FileOutputFormat.setOutputPath(job2, new Path(args[2]));
                System.exit(job2.waitForCompletion(true)? 0 : 1 );
          }
        
    }
    
}
