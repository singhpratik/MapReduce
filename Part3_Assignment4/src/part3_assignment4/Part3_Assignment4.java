/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part3_assignment4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class Part3_Assignment4 {
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();
        Job job  = Job.getInstance(conf,"STDMedian");
        job.setJarByClass(Part3_Assignment4.class);
        job.setMapperClass(Part3_Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(Part3_Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianSD.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
    
}
