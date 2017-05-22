/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joinflights;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_JoinSource extends Mapper<LongWritable,Text,Text,Text>{
private Text outValue = new Text();
private Text outKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //    super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        
      // System.out.print(value);
      
        String inputArrayString[] = value.toString().split(",");
        System.out.println(inputArrayString[0]);
        
        if(inputArrayString.length<29){
            System.out.print("Some columns are missing for the record");
        }else if (inputArrayString[0].equals("Year")){
            System.out.print("Reading the Column name record");
        }else if((Integer.parseInt(inputArrayString[1])==1)&&Integer.parseInt(inputArrayString[2])==1){
            String outputValue = inputArrayString[16]+"\t"+inputArrayString[17];
            outValue.set("A"+outputValue);
            outKey.set(inputArrayString[16]);
            context.write(outKey, outValue);
        }
        
    }
    
    
    
    
}
