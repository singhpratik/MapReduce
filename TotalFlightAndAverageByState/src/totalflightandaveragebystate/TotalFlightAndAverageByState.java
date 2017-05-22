/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package totalflightandaveragebystate;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class TotalFlightAndAverageByState {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
          Configuration conf  = new Configuration();
            Job job  = Job.getInstance(conf, "Average  By State");
            job.setJarByClass(TotalFlightAndAverageByState.class);
            job.setMapperClass(Mapper_State.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CompositeValue_State.class);
            job.addCacheFile(new Path(args[0]).toUri());
            job.setReducerClass(Reducer_State.class);
           // job.setNumReduceTasks(3);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CompositeValue_State.class);
           // job.setPartitionerClass(AverageDelay_Partitioner.class);
            FileInputFormat.addInputPath(job,new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true)?0:1);
    }
    
}
