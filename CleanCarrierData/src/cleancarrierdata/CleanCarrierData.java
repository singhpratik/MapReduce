/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cleancarrierdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class CleanCarrierData {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf  = new Configuration();
       Job job  = Job.getInstance(conf, "Clean aircraft details data");
       job.setJarByClass(CleanCarrierData.class);
       job.setMapperClass(Mapper_CleanAircraft.class);
       job.setMapOutputKeyClass(CompositeKey_AircraftDetails.class);
      // job.setCombinerClass(BusyRoute_Reducer.class);
       job.setMapOutputValueClass(NullWritable.class);
       //job.setReducerClass(Reducer_AllFlights.class);       
       //  job.setNumReduceTasks(1);
       job.setOutputKeyClass(CompositeKey_AircraftDetails.class);
       job.setOutputValueClass(NullWritable.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       System.exit( job.waitForCompletion(true)?1:0);
    }
    
}
