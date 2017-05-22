/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top10busycity;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Top10_Mapper_Outbound extends Mapper<LongWritable,Text,Text,IntWritable>{
    
    
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        
        
        String inputArray[] = value.toString().split(",");
        if(inputArray.length<29){
            System.out.print("Some columns are missing for the record");
        }else if (inputArray[0].equals("Year")){
            System.out.print("Reading the Column name record");
        }else{
            outputKey.set(inputArray[17]);
            outputValue.set(1);
            context.write(outputKey,outputValue);
        }
        
    
    }
    
    
    
    
    
    
}
