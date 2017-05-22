/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storeairportrecordinpig;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_StoreInPig extends Mapper<LongWritable,Text,Text,Text>{

    private Text outKey = new Text();
    private Text outValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //  super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
      
      String inputArray[] = value.toString().replace("\"", "").split(",");
      
      if(inputArray.length<7||inputArray[0].replace("\"", "").equals("iata")){
        System.out.println("Reading Columns");
      }else if(inputArray[0].equals("")||inputArray[1].equals("")){
         System.out.println("Reading empty value");
      }else{
          String valueToStore=inputArray[1].replace("\"", "")+" "+inputArray[5].replace("\"", "")+" "+inputArray[6].replace("\"", "");
          outKey.set(inputArray[0].replace("\"", ""));
          outValue.set(valueToStore);
          context.write(outKey, outValue);
      }
      
    }
    
    
    
    
    
    
}
