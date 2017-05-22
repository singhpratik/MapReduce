/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joinflights;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_AirportFile extends Mapper<Object,Text,Text,Text>{

    private Text outValue = new Text();
    private Text outKey = new Text();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
       String inputArray[] = value.toString().split(",");
       
       if(inputArray.length<6){
            System.out.print("Some columns are missing for the record");
        }else if (inputArray[0].replace("\"","").equals("iata")){
            System.out.print("Reading the Column name record");
        }else{
            String outputValue = inputArray[1].replace("\"","")+"\t"+inputArray[3].replace("\"","")+"\t"+inputArray[5].replace("\"","")+"\t"+inputArray[6].replace("\"","");
            outKey.set(inputArray[0].replace("\"",""));
            outValue.set("B"+outputValue);
            context.write(outKey,outValue);
        }
    }  
}
