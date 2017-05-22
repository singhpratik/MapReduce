/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package predictaveragedelay;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author pratik
 */
public class PredictAverageDelay {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
            Configuration conf  = new Configuration();
            Job job  = Job.getInstance(conf, "Predictions");
            job.setJarByClass(PredictAverageDelay.class);
            job.setMapperClass(Predict_Mapper.class);
            job.setMapOutputKeyClass(Predict_CompositeKey.class);
            job.setMapOutputValueClass(Predict_CompositeValue.class);
            
            job.setReducerClass(Pridict_Reducer.class);
            
            job.setOutputKeyClass(Predict_CompositeKey.class);
            job.setOutputValueClass(Predict_CompositeValue.class);
            
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true)? 1:0);
    } 
}
