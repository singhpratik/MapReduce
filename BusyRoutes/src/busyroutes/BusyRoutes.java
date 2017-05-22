/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class BusyRoutes {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
    Configuration conf  = new Configuration();
    Job job  = Job.getInstance(conf, "Total Number of flights per route");
    job.setJarByClass(BusyRoutes.class);
    job.setMapperClass(BusyRoute_Mapper.class);
    job.setMapOutputKeyClass(BusyRoute_CompositeKey.class);
    job.setCombinerClass(BusyRoute_Reducer.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(BusyRoute_Reducer.class);       
    job.setOutputKeyClass(BusyRoute_CompositeKey.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean jobCompleted = job.waitForCompletion(true);

    if(jobCompleted){
            Configuration conf2 = HBaseConfiguration.create();
           // HTable table = new HTable(conf2, "AirportDetails");
            Job job2  = Job.getInstance(conf2, "Maximum busiest route");
            //setting the htable
          //ening htable
            job2.setJarByClass(BusyRoutes.class);
            job2.setMapperClass(BusyRoute_Final_Mapper.class);
            job2.setMapOutputKeyClass(BusyRoute_CompositeKey.class);
            //job2.setCombinerClass(BusyRoute_Reducer.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setReducerClass(BusyRoute_Final_Reducer.class);
            job2.setOutputKeyClass(BusyRoute_CompositeKey.class);
            job2.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job2,new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            boolean jobComplete = job2.waitForCompletion(true);
            
            if(jobComplete){
                
                Configuration config = HBaseConfiguration.create();
                Job job3 = new Job(config,"Read from Hbase and write");
                job3.setJarByClass(BusyRoute_Mapper_getDetails.class);    // class that contains mapper

                Scan scan = new Scan();
                scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
                scan.setCacheBlocks(false);  // don't set to true for MR jobs
                // set other scan attrs
                job3.addCacheFile(new Path(args[2]+"/part-r-00000").toUri());

                TableMapReduceUtil.initTableMapperJob(
                        "AirportDetails",      // input table
                        scan,	          // Scan instance to control CF and attribute selection
                        BusyRoute_Mapper_getDetails.class,   // mapper class
                        null,	          // mapper output key
                        null,	          // mapper output value
                        job3);
                
                FileOutputFormat.setOutputPath(job3, new Path(args[3]));
                
                job3.setNumReduceTasks(0);
                
                System.exit(job3.waitForCompletion(true)?0:1);
                
            }
        }
    
   
    
    
    }
}
