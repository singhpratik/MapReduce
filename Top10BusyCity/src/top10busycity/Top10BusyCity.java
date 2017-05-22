/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top10busycity;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class Top10BusyCity {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
    Configuration conf  = new Configuration();
    Job job  = Job.getInstance(conf, "Total Number of inbound flights");
    job.setJarByClass(Top10BusyCity.class);
    job.setMapperClass(Top10_Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(Top10_Reducer.class);       
  //  job.setNumReduceTasks(1);
    job.addCacheFile(new Path(args[0]).toUri());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
   //System.exit(job.waitForCompletion(true)?0:1);
    
    if(job.waitForCompletion(true)){
            Job job2  = Job.getInstance(conf, "Total Number of outbound flights");
            job2.setJarByClass(Top10BusyCity.class);
            job2.setMapperClass(Top10_Mapper_Outbound.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setReducerClass(Top10_Reducer.class); 
            job2.addCacheFile(new Path(args[0]).toUri());
           // job2.setCombinerClass(Top10_Reducer.class);
        //    job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job2,new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
          
         if(job2.waitForCompletion(true)){
                    
            Job job3  = Job.getInstance(conf, "Total Number of flights");
            job3.setJarByClass(Top10BusyCity.class);
            MultipleInputs.addInputPath(job3, new Path(args[2]),TextInputFormat.class, Top10_Mapper_Combine.class);
            MultipleInputs.addInputPath(job3, new Path(args[3]),TextInputFormat.class,Top10_Mapper_Combine.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(IntWritable.class);
            job3.setReducerClass(Top10_Reducer_Combine.class); 
            //job3.setCombinerClass(Top10_Reducer.class);
           
            job3.setNumReduceTasks(1);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);
            FileOutputFormat.setOutputPath(job3, new Path(args[4]));
            System.exit(job3.waitForCompletion(true)?1:0);
            
//            if(job3.waitForCompletion(true)){
//                
//                Job job4  = Job.getInstance(conf, "Top 10 busy city");
//                job4.setJarByClass(Top10BusyCity.class);
//                job4.setMapperClass(Top10_Mapper_Calculate.class);
//                job4.setMapOutputKeyClass(Top10CompositeKeyWritable.class);
//                job4.setMapOutputValueClass(NullWritable.class);
//                job4.setReducerClass(Top10_Reducer_Calculate.class); 
//                //job3.setCombinerClass(Top10_Reducer.class);
//                //  job4.setNumReduceTasks(1);
//                job4.addCacheFile(new Path(args[5]).toUri());
//                job4.setOutputKeyClass(Top10CompositeKeyWritable.class);
//                job4.setOutputValueClass(NullWritable.class);
//                FileInputFormat.addInputPath(job4,new Path(args[3]));
//                FileOutputFormat.setOutputPath(job4, new Path(args[4]));
//                System.exit(job4.waitForCompletion(true)? 0:1);
//                }
            }  
        }  
    }  
}
 