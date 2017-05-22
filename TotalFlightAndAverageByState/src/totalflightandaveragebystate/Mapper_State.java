/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package totalflightandaveragebystate;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_State extends Mapper<LongWritable,Text,Text,CompositeValue_State>{

    private Text outKey = new Text();
    private CompositeValue_State outValue= new CompositeValue_State();
    
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
                         
                          if(!deptFieldArray[0].equals("iata")){
                                //String inValue=deptFieldArray[1].trim()+"\t"+deptFieldArray[5].trim()+"\t"+deptFieldArray[6].trim();
                                System.out.println("The key value is"+deptFieldArray[0]);
                                System.out.println("The out value is"+deptFieldArray[3]);
                                airports.put(deptFieldArray[0].trim(),deptFieldArray[3].trim());
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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        
        String inputString[] = value.toString().split(",");
        
        if(inputString.length<29||inputString[14].equals("NA")){
            System.out.println("Reading Columns");
        }else{
            
            if(airports.containsKey(inputString[17])){
                String state = airports.get(inputString[17]) ;
                outKey.set(state);
                outValue.setArrivalDelay(Double.parseDouble(inputString[14]));
                outValue.setCount(1);
                context.write(outKey, outValue);
            }
        
        }
        
        
    }
    
    
    
    
}
