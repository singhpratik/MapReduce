/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cleancarrierdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_CleanAircraft extends Mapper<LongWritable,Text,CompositeKey_AircraftDetails,NullWritable>{

    private CompositeKey_AircraftDetails outKey = new CompositeKey_AircraftDetails();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
 
       String inputArray[] = value.toString().replace("\"", "").split(",");
       
       if(inputArray.length<9||inputArray[0].equals("tailnum")){
           System.out.println("Invalid record");
       }else if(inputArray[8].equals("None")||inputArray[8].equals("")||inputArray[0].equals("")||inputArray[4].equals("")){
            System.out.println("Invalid Year");
       }else{
          outKey.setAircraftType(inputArray[4]);
          outKey.setTailNumber(inputArray[0]);
          outKey.setYear(Integer.parseInt(inputArray[8]));
          context.write(outKey, NullWritable.get());
       }
    }
   
}
