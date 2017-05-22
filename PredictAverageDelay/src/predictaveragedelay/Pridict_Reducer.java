/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package predictaveragedelay;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Pridict_Reducer extends Reducer<Predict_CompositeKey,Predict_CompositeValue,Predict_CompositeKey,Predict_CompositeValue>{
    
    private Predict_CompositeValue result = new Predict_CompositeValue();
    
    @Override
    protected void reduce(Predict_CompositeKey key, Iterable<Predict_CompositeValue> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        
//        Double sum = 0.0;
//        Integer count =0;
//        
        for(Predict_CompositeValue val:values){
//            sum = sum+val.getAvereage();
//            count = count+val.getCount();   
              context.write(key, val);

        }
//        result.setAvereage(sum/count);
//        result.setCount(count);
        
    }
    
    
    
}
