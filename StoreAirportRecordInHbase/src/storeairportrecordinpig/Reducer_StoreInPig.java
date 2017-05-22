/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storeairportrecordinpig;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author pratik
 */
public class Reducer_StoreInPig extends  TableReducer<Text,Text,ImmutableBytesWritable>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        for(Text val:values){
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("Airports"),Bytes.toBytes(key.toString()),Bytes.toBytes(val.toString()));
            context.write(null, put);
        }
    }
}
