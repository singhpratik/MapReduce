/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class BusyRoute_Final_Reducer extends Reducer<BusyRoute_CompositeKey, IntWritable, BusyRoute_CompositeKey, IntWritable> {

    private IntWritable maxCount  = new IntWritable(0);
    private BusyRoute_CompositeKey result = new BusyRoute_CompositeKey();
      String name;
   //  HTable table;
    @Override
    protected void reduce(BusyRoute_CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       
        int sum =0;
        for(IntWritable val:values){
            sum = sum+val.get();
        }
        //finding out the max value and setting the results
        if(maxCount.get()==0||maxCount.get()<sum){
            maxCount.set(sum);
            result.setOrigin(key.getOrigin());
            result.setDestination(key.getDestination());
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
       // String origin=getFromHbase(result.getOrigin(),context);
        context.write(result, maxCount);
    }
    
}
