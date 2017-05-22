/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import javax.annotation.concurrent.Immutable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author pratik
 */
public class BusyRoute_Mapper_getDetails extends TableMapper<Text,IntWritable>{
 
    
    private final IntWritable count = new IntWritable(1);
    private Text text = new Text();
    private String source;
    private String destination;
    private Integer totalCount;
    
    private  String sourceName="";
    private String destinationName="";
    
    
    //############### reading from Distributed Cache
    
     // private static HashMap<String, String> airports = new HashMap<String, String>();
      private BufferedReader brReader;
    
      @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFilesLocal = context.getLocalCacheFiles();
         System.out.print("The lentgh is "+cacheFilesLocal.length);
        for (Path eachPath : cacheFilesLocal) {
        if (eachPath.getName().toString().trim().equals("part-r-00000")) {
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
                        String deptFieldArray[] = strLineRead.replace("\"", "").split("\\t");
                         System.out.println("The value is"+deptFieldArray[0]);
                          //if(!deptFieldArray[0].equals("iata")){
                                 source = deptFieldArray[0].trim();
                                 destination = deptFieldArray[1].trim();
                                 totalCount = Integer.parseInt(deptFieldArray[2].trim());
                         // }
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

    
       //############### reading from Distributed Cache
    
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            
           if(Bytes.toString(row.get()).equals(source)){
               System.out.println("Inside source Row Hbase");
              sourceName = new String(value.getValue(Bytes.toBytes("Airports"), Bytes.toBytes(source)));
           }else if(Bytes.toString(row.get()).equals(destination)){
                System.out.println("Inside Destination Row Hbase");
               destinationName =  new String(value.getValue(Bytes.toBytes("Airports"), Bytes.toBytes(destination)));
           }
   	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
  
           if(!sourceName.equals("")&&!destinationName.equals("")){
                text.set(sourceName+"   "+destinationName);
                count.set(totalCount);
                context.write(text, count);
           }
    }  
}
