/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package carrierpopularity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Carrier_Reducer extends Reducer<Carrier_CompositeKey,IntWritable,Carrier_CompositeKey,IntWritable>{

    
    private Carrier_CompositeKey resultKey = new Carrier_CompositeKey();
    private IntWritable resultValue = new IntWritable();
    
     private static HashMap<String, String> carrier = new HashMap<String, String>();
    private BufferedReader brReader;
    
      @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFilesLocal = context.getLocalCacheFiles();
         System.out.print("The lentgh is "+cacheFilesLocal.length);
        for (Path eachPath : cacheFilesLocal) {
        if (eachPath.getName().toString().trim().equals("carriers.csv")) {
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
                          if(!deptFieldArray[0].equals("Code")){
                                 carrier.put(deptFieldArray[0].trim(),deptFieldArray[1].trim());
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
    protected void reduce(Carrier_CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      
        int sum  =0;
        for(IntWritable val:values){
            sum =sum+1;
        }
        
        resultKey.setCarrierName(carrier.get(key.getCarrierName()));
        resultKey.setYear(key.getYear());
        
        resultValue.set(sum);
      context.write(resultKey, resultValue);
      
    
    }
 
}
