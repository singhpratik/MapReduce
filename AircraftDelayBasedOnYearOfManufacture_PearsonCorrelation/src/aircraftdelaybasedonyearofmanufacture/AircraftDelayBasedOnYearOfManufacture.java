/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aircraftdelaybasedonyearofmanufacture;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class AircraftDelayBasedOnYearOfManufacture {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf  = new Configuration();
        Job job  = Job.getInstance(conf, "Finding Average for pearson correlation");
        job.setJarByClass(AircraftDelayBasedOnYearOfManufacture.class);
        job.setMapperClass(Mapper_AverageDelay.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CompositeValue_Average.class);
        job.addCacheFile(new Path(args[0]).toUri());//input path to caches file
        job.setReducerClass(Reducer_AverageDelay.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(CompositeValue_Average.class);
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean jobCompleted = job.waitForCompletion(true);
        
        if(jobCompleted){
        
          // Configuration c = new Configuration();
           Job job2 = Job.getInstance(conf, "Finding the pearson Coeeficient");
           job2.setJarByClass(AircraftDelayBasedOnYearOfManufacture.class);
           job2.setMapperClass(Pearson_Mapper.class);
           job2.setReducerClass(Pearson_Reducer.class);
           job2.setMapOutputKeyClass(Pearson_KeyWritable.class);
           job2.setMapOutputValueClass(Pearson_ValueWritable.class);
           job2.setOutputKeyClass(Text.class);
           job2.setOutputValueClass(DoubleWritable.class);
           FileInputFormat.addInputPath(job2, new Path(args[2]));
           FileOutputFormat.setOutputPath(job2, new Path(args[3]));
           System.exit(job2.waitForCompletion(true)? 0:1);
        }
    }
    
}
