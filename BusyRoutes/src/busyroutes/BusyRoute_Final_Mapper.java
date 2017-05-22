/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class BusyRoute_Final_Mapper extends Mapper<LongWritable,Text,BusyRoute_CompositeKey,IntWritable>{

    private IntWritable outValue = new IntWritable();
    
    private BusyRoute_CompositeKey outKey = new BusyRoute_CompositeKey();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
       String inputArray[] = value.toString().split("\\t");
       outKey.setOrigin(inputArray[0]);
       outKey.setDestination(inputArray[1]);
       outValue.set(Integer.parseInt(inputArray[2]));
       context.write(outKey, outValue);
    }
    
}
