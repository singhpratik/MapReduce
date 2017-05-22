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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Top10_Reducer_Calculate extends Reducer<Top10CompositeKeyWritable,NullWritable,Top10CompositeKeyWritable,NullWritable>{
    
    private Top10CompositeKeyWritable result = new Top10CompositeKeyWritable();
    private int count=0;
    
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
                                 airports.put(deptFieldArray[0].trim(),deptFieldArray[1].trim());
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
    protected void reduce(Top10CompositeKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        count++;
      if(count<11) {
        result.setCity(airports.get(key.getCity()));
        result.setTotalNumberOfFlights(key.getTotalNumberOfFlights());
        context.write(result, NullWritable.get());
      }
    }
    
}
