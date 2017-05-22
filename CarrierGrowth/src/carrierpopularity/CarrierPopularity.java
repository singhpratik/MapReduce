/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package carrierpopularity;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class CarrierPopularity {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
    Configuration conf  = new Configuration();
    Job job  = Job.getInstance(conf, "Total Number of inbound flights");
    job.setJarByClass(CarrierPopularity.class);
    job.setMapperClass(Mapper_Carrier.class);
    job.setMapOutputKeyClass(Carrier_CompositeKey.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(Carrier_Reducer.class);       
  //  job.setNumReduceTasks(1);
    job.addCacheFile(new Path(args[0]).toUri());
    job.setOutputKeyClass(Carrier_CompositeKey.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true)?0:1);
    }
    
}
