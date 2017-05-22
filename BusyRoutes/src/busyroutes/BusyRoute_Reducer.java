/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class BusyRoute_Reducer extends Reducer<BusyRoute_CompositeKey,IntWritable,BusyRoute_CompositeKey,IntWritable>{

  //  private BusyRoute_CompositeKey result = new BusyRoute_CompositeKey();
    private IntWritable count  = new IntWritable();
    
    
    @Override
    protected void reduce(BusyRoute_CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        
        int sum =0;
        for(IntWritable val:values){
            sum = sum +val.get();
        }
        
        count.set(sum);
        context.write(key, count);
    
    }
    
    
    
    
}
