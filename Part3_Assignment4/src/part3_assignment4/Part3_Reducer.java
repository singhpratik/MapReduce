/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part3_assignment4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Part3_Reducer extends Reducer<IntWritable,IntWritable,IntWritable,MedianSD>{

    MedianSD result = new MedianSD();
    ArrayList<Double> inMemory = new ArrayList<>();
    
    @Override
    protected void  reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count =0;
            
            inMemory.clear();
            result.setMedian(0);
            result.setStdDev(0);
            
            for(IntWritable val:values){
                inMemory.add((double)val.get());
                sum = sum + val.get();
                count++;
            }
            
            Collections.sort(inMemory);
            
            if(count%2==0){
                result.setMedian((inMemory.get((int) (count/2 - 1))+inMemory.get((int)(count/2)))/2);
            }else{
                result.setMedian(inMemory.get((int)count/2));
            }
            
            double mean = sum/count;
            double sumOfSquares = 0;
            for(Double f : inMemory){
                sumOfSquares +=(f-mean)*(f-mean);
            }
            
            result.setStdDev((double)Math.sqrt(sumOfSquares/(count-1)));
            context.write(key, result);
    }
}
