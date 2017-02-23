/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wordcount_lab1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */

// THis is Mapper Class
public class WordCount_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        StringTokenizer itr = new StringTokenizer(value.toString());//used for tokenizing the individual words
        //the output will be Word and value is 1
        //By default the string tokenizer breaks the words with spaces
        //To transfer the data, the serialised text needs to be converted into String

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }

    }
}
