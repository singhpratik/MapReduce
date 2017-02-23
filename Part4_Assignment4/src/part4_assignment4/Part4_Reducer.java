/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part4_assignment4;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Part4_Reducer extends Reducer<IntWritable,SortedMapWritable,IntWritable,MedianStd>{
    
    MedianStd result = new MedianStd();
    TreeMap<Integer,Integer> valueCounts = new TreeMap<>();
    
    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        float sum=0;
        int totalValues = 0;
        valueCounts.clear();
        result.setMedian(0);
        result.setStdDev(0);
        
        for (SortedMapWritable v : values) {
            
            for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
                
                int numberOfValues = ((IntWritable) entry.getKey()).get();
                
                int count = ((IntWritable) entry.getValue()).get();
                
                 totalValues += count;
                 sum += (numberOfValues * count);
                 
                 Integer storedCount = valueCounts.get(numberOfValues);
                 
                 if (storedCount == null) {
                       valueCounts.put(numberOfValues, count); 
                  }else{
                       valueCounts.put(numberOfValues, (storedCount + count));
                  }
            } 
            v.clear();
        }

        int medianIndex = totalValues/2;
        int previousCount = 0;
        int counts = 0;
        int prevKey = 0;
        boolean check =false;
        for (Entry<Integer, Integer> entry : valueCounts.entrySet()) {
            counts = previousCount + entry.getValue();
            if (previousCount <= medianIndex && medianIndex < counts) {
                
                if (((totalValues % 2) == 0) && (previousCount == medianIndex)) {
                      result.setMedian((double)(entry.getKey()+prevKey)/2);
                    }else{ 
                      result.setMedian(entry.getKey());
                    }
                check = true;
                  
            }
            
            previousCount = counts;
            prevKey = entry.getKey();
            
            if(check)
                break; 
        }
        
        double mean =(double)sum/totalValues;
        double sumOfSquares = 0;
        
            for (Entry<Integer, Integer> entry : valueCounts.entrySet()) {
                sumOfSquares += ((entry.getKey() - mean)*(entry.getKey() - mean)*entry.getValue());
            }
            result.setStdDev((double) Math.sqrt(sumOfSquares/(totalValues-1)));
            context.write(key, result);
    }
}
