/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package totalflightandaveragebystate;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Reducer_State extends Reducer<Text,CompositeValue_State,Text,CompositeValue_State>{

    private CompositeValue_State result = new CompositeValue_State();
    
    
    
    @Override
    protected void reduce(Text key, Iterable<CompositeValue_State> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        Double sum =0.0;
        Integer count =0;
        
        for(CompositeValue_State val :values){
            sum = sum +val.getArrivalDelay();
            count= count+val.getCount();
        }
        
        result.setArrivalDelay(sum/count);
        result.setCount(count);
        context.write(key, result);
    }
   
    
    
    
}
