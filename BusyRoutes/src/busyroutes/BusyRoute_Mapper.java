/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class BusyRoute_Mapper extends Mapper<LongWritable,Text,BusyRoute_CompositeKey,IntWritable>{

    private BusyRoute_CompositeKey outKey = new BusyRoute_CompositeKey();
    private IntWritable outValue = new IntWritable();
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        
        String inputArray[] = value.toString().split(",");
        if(inputArray[0].equals("Year")){
            System.out.println("reading the Column Name");
        }else{
            outKey.setOrigin(inputArray[16]);
            outKey.setDestination(inputArray[17]);
            outValue.set(1);
            context.write(outKey, outValue);
        }
    }
    
    
    
    
    
}
