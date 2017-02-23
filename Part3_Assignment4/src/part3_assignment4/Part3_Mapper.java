/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part3_assignment4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Part3_Mapper extends Mapper<Object, Text,IntWritable,IntWritable>{
    IntWritable inputKey = new IntWritable();
    IntWritable ratings = new IntWritable();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     //   super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
     String[] inputArray = value.toString().split("::");
        inputKey.set(Integer.parseInt(inputArray[1]));
        ratings.set(Integer.parseInt(inputArray[2]));
        context.write(inputKey, ratings);
    } 
}
