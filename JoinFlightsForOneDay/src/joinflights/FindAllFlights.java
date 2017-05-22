    /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joinflights;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class FindAllFlights {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();
        Job job  = Job.getInstance(conf, "Inner join");
        job.setJarByClass(FindAllFlights.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,Mapper_JoinSource.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,Mapper_AirportFile.class);
       // job.setNumReduceTasks(0);
        job.setReducerClass(Reducer_Join.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //System.exit(job.waitForCompletion(true)?1:0);
        boolean jobCompleted = job.waitForCompletion(true);
        
        if(jobCompleted){
        
            Job job2  = Job.getInstance(conf, "Inner join");
            job2.setJarByClass(FindAllFlights.class);
            MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class,Mapper_Destination.class);
            MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class,Mapper_AirportFile.class);
            // job.setNumReduceTasks(0);
            job2.setReducerClass(Reducer_Join.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            System.exit(job2.waitForCompletion(true)?1:0);
       
        }
    }
    
}
