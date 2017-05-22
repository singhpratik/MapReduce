/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aircraftdelaybasedonyearofmanufacture;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Pearson_Mapper extends Mapper<LongWritable,Text,Pearson_KeyWritable,Pearson_ValueWritable>{

    private Pearson_KeyWritable outKey = new Pearson_KeyWritable();
    private Pearson_ValueWritable outValue = new Pearson_ValueWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        
               String inputArray[] = value.toString().split("\\t");
               
               int i=0;
               int j=0;
               
               while(i<inputArray.length){
                   j=i+1;
                   while(j<inputArray.length){
                       outKey.setRowIndex(i);
                       outKey.setColumnindex(j);
                       outValue.setElement1(Double.parseDouble(inputArray[i]));
                       outValue.setElement2(Double.parseDouble(inputArray[j]));
                       context.write(outKey, outValue);
                       j++;
                   }
                   i++;
               }
    }
    
    
    
    
    
}
