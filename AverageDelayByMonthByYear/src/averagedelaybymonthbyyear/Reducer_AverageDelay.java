/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagedelaybymonthbyyear;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Reducer_AverageDelay extends Reducer<CompositeKey_AverageDelay,CompositeValue_AverageDelay,CompositeKey_AverageDelay,CompositeValue_AverageDelay>{

    
    private CompositeKey_AverageDelay resultKey = new CompositeKey_AverageDelay();
    private CompositeValue_AverageDelay resultValue = new CompositeValue_AverageDelay();
    
    
    @Override
    protected void reduce(CompositeKey_AverageDelay key, Iterable<CompositeValue_AverageDelay> values, Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
    
       Double sum =0.0;
       int count =0;
       
       for(CompositeValue_AverageDelay val:values){
            sum  = sum +val.getAverageDelay();
            count = count+val.getCount();
       }
    
       resultValue.setAverageDelay(sum/count);
       resultValue.setCount(count);
       context.write(key, resultValue);
    
       
    }
    
}
