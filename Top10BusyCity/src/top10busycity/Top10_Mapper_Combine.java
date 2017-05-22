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
public class Top10_Mapper_Combine extends Mapper<LongWritable,Text,Text,IntWritable>{

    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputArray[] = value.toString().split("\\t");
        outputKey.set(inputArray[0]);
        outputValue.set(Integer.parseInt(inputArray[1]));
        context.write(outputKey, outputValue);
        
    }
}
