/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top10busycity;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Top10_Mapper_Calculate extends Mapper<LongWritable,Text,Top10CompositeKeyWritable,NullWritable>{

    private Top10CompositeKeyWritable outputKey = new Top10CompositeKeyWritable();
 
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //  super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
      String inputArray[] = value.toString().split("\\t");
      outputKey.setCity(inputArray[0]);
      outputKey.setTotalNumberOfFlights(Integer.parseInt(inputArray[1])); 
      context.write(outputKey,NullWritable.get());
    }  
    
}
