/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joinflights;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Reducer_Join extends Reducer<Text,Text,Text,Text>{
    
        
  private static final Text EMPTY_TEXT =new Text("");
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<>();
    private ArrayList<Text> listB = new ArrayList<>();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   
    listA.clear();
    listB.clear();
        
         for(Text val:values) {
            if (Character.toString((char) val.charAt(0)).equals("A")) {
                System.out.print("Inside A");
                listA.add(new Text(val.toString().substring(1)));
            } else if (Character.toString((char) val.charAt(0)).equals("B")) {
                      System.out.print("Inside B");
                listB.add(new Text(val.toString().substring(1)));
            }
        }
         System.out.println("The length is A is #####  "+listA.size());
         System.out.println("The length is B #### "+listB.size());
         executeJoinLogic(context);
    }
    
       public void executeJoinLogic(Context context) throws IOException, InterruptedException{
        if (!listA.isEmpty() && !listB.isEmpty()) {
        for (Text A : listA) {
                for (Text B : listB) {
                    context.write(A, B);
                }
             }
        }
    }
    
}
