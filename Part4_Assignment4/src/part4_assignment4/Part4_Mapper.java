/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part4_assignment4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Part4_Mapper extends Mapper<Object,Text,IntWritable,SortedMapWritable>{
    
    IntWritable count = new IntWritable(1);
    IntWritable inputKey = new IntWritable();
    IntWritable intValue = new IntWritable();//To store the value of input

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] inputArray = value.toString().split("::");
        inputKey.set(Integer.parseInt(inputArray[1]));
        intValue.set(Integer.parseInt(inputArray[2]));
        SortedMapWritable inputValue = new SortedMapWritable();
        inputValue.put(intValue, count);
        context.write(inputKey, inputValue);
    }
}
