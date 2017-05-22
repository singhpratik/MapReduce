/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package allflightswithinstates;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Reducer_AllFlights extends Reducer<Text,NullWritable,Text,NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
          context.write(key, NullWritable.get());
      }
}
