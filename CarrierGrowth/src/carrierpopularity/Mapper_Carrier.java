/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package carrierpopularity;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_Carrier extends Mapper<LongWritable,Text,Carrier_CompositeKey,IntWritable>{
    
    private Carrier_CompositeKey outKey = new Carrier_CompositeKey();
    private IntWritable outValue = new IntWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
       
       String inputArray[] = value.toString().split(",");
       
       if(inputArray[0].equals("Year")||inputArray.length<29||inputArray[8].equals("")){
       
       }else if(inputArray[8].equals("US")||inputArray[8].equals("WN")||inputArray[8].equals("UA")||inputArray[8].equals("CO")||inputArray[8].equals("AA")){
           
              outKey.setCarrierName(inputArray[8]);
              outKey.setYear(Integer.parseInt(inputArray[0]));
              outValue.set(1);
              context.write(outKey, outValue);
        }
    
    }
    
    
    
    
    
    
}
