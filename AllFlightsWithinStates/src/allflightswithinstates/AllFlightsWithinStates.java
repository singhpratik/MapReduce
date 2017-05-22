/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package allflightswithinstates;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author pratik
 * Description: Uses the Joined File saved in the Hadoop File system for Flight Details
 */
public class AllFlightsWithinStates {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
    Configuration conf  = new Configuration();
    Job job  = Job.getInstance(conf, "Flights from IL to CA");
    job.setJarByClass(AllFlightsWithinStates.class);
    job.setMapperClass(Mapper_AllFlights.class);
    job.setMapOutputKeyClass(Text.class);
   // job.setCombinerClass(BusyRoute_Reducer.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(Reducer_AllFlights.class);       
  //  job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit( job.waitForCompletion(true)?1:0);
    }
    
}
