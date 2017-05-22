/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aircraftdelaybasedonyearofmanufacture;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Reducer_AverageDelay extends Reducer<IntWritable,CompositeValue_Average,IntWritable,CompositeValue_Average>{
    
    private CompositeValue_Average result = new CompositeValue_Average();

    @Override
    protected void reduce(IntWritable key, Iterable<CompositeValue_Average> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        Double sum =0.0;
        int count=0;
        for(CompositeValue_Average val:values){
            sum = sum+val.getAverageDelay();
            count = count+val.getCount();
        }
        result.setAverageDelay(sum/count);
        result.setCount(count);
        context.write(key, result);
        
    }
    
    
    
    
    
    
}
