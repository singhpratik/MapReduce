/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top10busycity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Top10_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
   
    private IntWritable result = new IntWritable();
    //private boolean flag = false;
    
    
    private Text outputkey = new Text();
    private static HashMap<String, String> airports = new HashMap<String, String>();
    private BufferedReader brReader;
    
      @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFilesLocal = context.getLocalCacheFiles();
         System.out.print("The lentgh is "+cacheFilesLocal.length);
        for (Path eachPath : cacheFilesLocal) {
        if (eachPath.getName().toString().trim().equals("airports.csv")) {
           // context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
                    loadAirportsHashMap(eachPath, context);
                 }
            }
        }

        private void loadAirportsHashMap(Path filePath, Context context) throws IOException {
                

                        String strLineRead = "";

        try {
        brReader = new BufferedReader(new FileReader(filePath.toString()));

        // Read each line, split and load to HashMap
        while ((strLineRead = brReader.readLine()) != null) {
                System.out.println("Inside buffer");
                        String deptFieldArray[] = strLineRead.replace("\"", "").split(",");
                         System.out.println("The value is"+deptFieldArray[0]);
                          if(!deptFieldArray[0].equals("iata")){
                                String inValue=deptFieldArray[1].trim()+"\t"+deptFieldArray[5].trim()+"\t"+deptFieldArray[6].trim();
                                 airports.put(deptFieldArray[0].trim(),inValue);
                          }
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                    e.printStackTrace();
                    }finally {
                    if (brReader != null) {
                     brReader.close();
                }
            }
        }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
       int sum=0;
       for(IntWritable val:values){
           sum =sum+1;
       }
       
       if(airports.containsKey(key.toString())){
        outputkey.set(airports.get(key.toString()));
        result.set(sum);
        context.write(outputkey, result);
       }
    }
    
}
