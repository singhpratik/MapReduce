/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aircraftdelaybasedonyearofmanufacture;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author pratik
 */
public class Mapper_AverageDelay extends Mapper<LongWritable,Text,IntWritable,CompositeValue_Average>{
    
     private static HashMap<String, String> airCraft = new HashMap<String, String>();
    private BufferedReader brReader;
  //  private String strDeptName ="";
    private IntWritable outKey = new IntWritable();
    private CompositeValue_Average outValue = new CompositeValue_Average();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFilesLocal = context.getLocalCacheFiles();
         System.out.print("The lentgh is "+cacheFilesLocal.length);
        for (Path eachPath : cacheFilesLocal) {
        if (eachPath.getName().toString().trim().equals("part-r-00000")) {
           // context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
                    System.out.println("Inside part-r-0000 present");
                    loadAircraftHashMap(eachPath, context);
                 }
            }
        }

        private void loadAircraftHashMap(Path filePath, Context context) throws IOException {
                

                        String strLineRead = "";

        try {
        brReader = new BufferedReader(new FileReader(filePath.toString()));

        // Read each line, split and load to HashMap
        while ((strLineRead = brReader.readLine()) != null) { 
                        String deptFieldArray[] = strLineRead.replace("\"", "").split("\t");
                                 airCraft.put(deptFieldArray[1].trim(),deptFieldArray[0].trim());
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
      //  super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
      
      
      String inputString[] = value.toString().split(",");
      
      if(inputString[0].equals("Year")||inputString.length<29||inputString[10].equals("")||inputString[15].equals("NA")){
         System.out.println("Reading Columns");    
      }else{
          if(airCraft.containsKey(inputString[10])){
              System.out.println("The key is "+inputString[10]);
              String year = airCraft.get(inputString[10]);
              outKey.set(Integer.parseInt(year));
              outValue.setAverageDelay(Double.parseDouble(inputString[15]));
              outValue.setCount(1);
              context.write(outKey, outValue);
          }
      } 
   }
}
