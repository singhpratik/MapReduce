/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package part4_assignment4;

import java.io.IOException;
import java.util.Map.Entry;
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
public class ItrCombiner extends Reducer<IntWritable,SortedMapWritable,IntWritable,SortedMapWritable>{

    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
    //    super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
  
        SortedMapWritable sortedMapOutputVal = new SortedMapWritable();
        for(SortedMapWritable val:values){
                
              for (Entry<WritableComparable, Writable> entry : val.entrySet()) {
                  
                  IntWritable count = (IntWritable) sortedMapOutputVal.get(entry.getKey());
                  if(count!=null){
                      count.set(count.get()+((IntWritable) entry.getValue()).get());
                  }else{
                      sortedMapOutputVal.put(entry.getKey(), new IntWritable(((IntWritable) entry.getValue()).get()));
                  }
                  val.clear();
              }
        }
        context.write(key, sortedMapOutputVal);
    } 
    
}
