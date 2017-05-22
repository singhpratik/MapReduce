/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storeairportrecordinpig;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author pratik
 */
public class StoreAirportRecordInPig {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here       
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config,"Example");
        job.setJarByClass(StoreAirportRecordInPig.class);
        Scan scan  = new Scan();
        scan.setCaching(500);
        job.setMapperClass(Mapper_StoreInPig.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        TableMapReduceUtil.initTableReducerJob("AirportDetails",Reducer_StoreInPig.class, job);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        System.exit(job.waitForCompletion(true)?1:0);
    }
    
}
