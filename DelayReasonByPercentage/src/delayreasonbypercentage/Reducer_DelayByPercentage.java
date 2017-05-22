
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package delayreasonbypercentage;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Reducer_DelayByPercentage extends Reducer<IntWritable,DelayPercentage_CompositeKey,IntWritable,DelayPercentage_CompositeKey>{
    
    private DelayPercentage_CompositeKey result = new DelayPercentage_CompositeKey();
    
    @Override
    protected void reduce(IntWritable key, Iterable<DelayPercentage_CompositeKey> values, Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
   
       result.setCarrierDelay(0);
       result.setWeatherDelay(0);
       result.setNasDelay(0);
       result.setSecurityDelay(0);
       result.setLateAirDelay(0);
       result.setTotalDelay(0);
       
       for(DelayPercentage_CompositeKey val:values){
//           
//           if(result.getCarrierDelay()==null){
//                result.setCarrierDelay(val.getCarrierDelay());
//                result.setWeatherDelay(val.getWeatherDelay());
//                result.setNasDelay(val.getNasDelay());
//                result.setSecurityDelay(val.getSecurityDelay());
//                result.setLateAirDelay(val.getLateAirDelay());
//                result.setTotalDelay(val.getTotalDelay());
//           }else{
                result.setCarrierDelay(result.getCarrierDelay()+val.getCarrierDelay());
                result.setWeatherDelay(result.getWeatherDelay()+val.getWeatherDelay());
                result.setNasDelay(result.getNasDelay()+val.getNasDelay());
                result.setSecurityDelay(result.getSecurityDelay()+val.getSecurityDelay());
                result.setLateAirDelay(result.getLateAirDelay()+val.getLateAirDelay());
                result.setTotalDelay(result.getCarrierDelay()+result.getWeatherDelay()+result.getLateAirDelay()+result.getNasDelay()+result.getSecurityDelay());
      
                //  }
       }
       
        
       context.write(key, result);
    }
    
    
    
}
