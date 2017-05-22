/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package allflightswithinstates;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_AllFlights extends Mapper<LongWritable,Text,Text,NullWritable>{

    private Text outKey = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //  super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        String inputString[] = value.toString().split("\\t");
        if(inputString[3].equals("IL")&&inputString[7].equals("CA")){
            context.write(value,NullWritable.get());
        }
    }
}
