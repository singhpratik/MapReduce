/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part4_assignment4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class Part4_Assignment4 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
         Configuration conf = new Configuration();
        Job job  = Job.getInstance(conf,"STDMedian Using Combiner");
        job.setJarByClass(Part4_Assignment4.class);
        job.setMapperClass(Part4_Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setCombinerClass(ItrCombiner.class);
        job.setReducerClass(Part4_Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianStd.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
    
}
