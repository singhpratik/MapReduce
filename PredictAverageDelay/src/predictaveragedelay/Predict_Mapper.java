/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package predictaveragedelay;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Predict_Mapper extends Mapper<LongWritable,Text,Predict_CompositeKey,Predict_CompositeValue>{

    private Predict_CompositeKey outKey = new Predict_CompositeKey();
    private Predict_CompositeValue outValue = new Predict_CompositeValue();
  
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
       String inputValue[] = value.toString().split(",");
       
       if(inputValue.length<29){
           System.out.println("Invalid column");
       }
       else if(inputValue[0].equals("Year")){
           System.out.println("Reading the column names");
       }else if(!inputValue[15].equals("NA")){
           outKey.setFlightNumber(Integer.parseInt(inputValue[9]));
           outKey.setDayOfMonth(Integer.parseInt(inputValue[2]));
           outKey.setMonth(Integer.parseInt(inputValue[1]));
           
           outValue.setCount(1);
           outValue.setAvereage(Double.parseDouble(inputValue[15]));
           
           context.write(outKey, outValue);
       }
    }
    
}
