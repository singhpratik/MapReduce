/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joinflights;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_Destination extends Mapper<LongWritable,Text,Text,Text>{
    
    private Text outKey = new Text();
    private Text outValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String inputArray[] = value.toString().split("\\t");    
            outKey.set(inputArray[1]);
            outValue.set("A"+value);
            context.write(outKey, outValue);  
    }   
}
