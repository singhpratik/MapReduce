/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagedelaybymonthbyyear;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_AverageDelay extends Mapper<LongWritable,Text,CompositeKey_AverageDelay,CompositeValue_AverageDelay>{

    private CompositeKey_AverageDelay outKey = new CompositeKey_AverageDelay();
    private CompositeValue_AverageDelay outValue = new CompositeValue_AverageDelay();
    private HashMap<Integer,String> months = new HashMap<>();
 
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       months.put(1, "January");
       months.put(2, "February");
       months.put(3, "March");
       months.put(4, "April");
       months.put(5, "May");
       months.put(6, "June");
       months.put(7, "July");
       months.put(8, "August");
       months.put(9, "September");
       months.put(10,"October");
       months.put(11,"November");
       months.put(12,"December");    
    }
   
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //  super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
      
            String inputArray[] = value.toString().split(",");
            
            if(inputArray.length<29||inputArray[0].equals("Year")){
                System.out.println("Reading columns");
            }else if(inputArray[0].equals("")||inputArray[1].equals("")||inputArray[1].equals("NA")){
                System.out.println("Invalid columns");
            }else if((!inputArray[21].equals("1"))&&(!inputArray[15].equals("NA"))){
                
                
                String outMonth = months.get(Integer.parseInt(inputArray[1]));
                
                if(Double.parseDouble(inputArray[15])>15.0){
                    //setting the put value with average and count
                    outValue.setAverageDelay(Double.parseDouble(inputArray[15]));
                    outValue.setCount(1);
                    
                    outKey.setMonth(outMonth);
                    outKey.setYear(Integer.parseInt(inputArray[0])); 
                    context.write(outKey, outValue);
                }
            }
         }
    }
