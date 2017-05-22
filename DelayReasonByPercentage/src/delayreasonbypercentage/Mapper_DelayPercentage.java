/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package delayreasonbypercentage;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_DelayPercentage extends Mapper<LongWritable,Text,IntWritable,DelayPercentage_CompositeKey>{

    IntWritable outKey = new IntWritable();
    DelayPercentage_CompositeKey outValue = new DelayPercentage_CompositeKey();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
       
       String inputArray[] = value.toString().split(",");
       
       if(inputArray.length<29){
           System.out.println("Invalid Data");
       }else if(inputArray[0].equals("Year")||inputArray[0].equals("")){
           System.out.println("Reading column headers");
       }else if((!inputArray[24].equals("NA"))&&(!inputArray[25].equals("NA"))&&(!inputArray[26].equals("NA"))&&(!inputArray[27].equals("NA"))&&(!inputArray[28].equals("NA"))){
           outKey.set(Integer.parseInt(inputArray[0]));
           outValue.setCarrierDelay(Integer.parseInt(inputArray[24]));
           outValue.setWeatherDelay(Integer.parseInt(inputArray[25]));
           outValue.setNasDelay(Integer.parseInt(inputArray[26]));
           outValue.setSecurityDelay(Integer.parseInt(inputArray[27]));
           outValue.setLateAirDelay(Integer.parseInt(inputArray[28]));
           outValue.setTotalDelay(0);
           context.write(outKey, outValue);
       }
    } 
}
