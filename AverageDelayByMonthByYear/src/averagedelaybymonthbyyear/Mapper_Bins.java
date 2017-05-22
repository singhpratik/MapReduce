/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagedelaybymonthbyyear;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author pratik
 */
public class Mapper_Bins extends Mapper<LongWritable,Text,Text,NullWritable>{

     private MultipleOutputs<Text, NullWritable> mos = null;
     private Text inputHour = new Text();
     
     
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      //  super.setup(context); //To change body of generated methods, choose Tools | Templates.
   
             mos = new MultipleOutputs(context);

    }
    
     

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //  super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
      String inputArray[]= value.toString().split("\\t");

      if(inputArray[0].equals("2008")){
           mos.write("bins", value, NullWritable.get(),"2008");
      }else if(inputArray[0].equals("2007")){
            mos.write("bins", value, NullWritable.get(),"2007");
      }else if(inputArray[0].equals("2006")){
            mos.write("bins", value, NullWritable.get(),"2006");
      }else if(inputArray[0].equals("2005")){
            mos.write("bins", value, NullWritable.get(),"2005");
      }else if(inputArray[0].equals("2004")){
            mos.write("bins", value, NullWritable.get(),"2004");
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      //  super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
      mos.close();
    }
    
}
