/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aircraftdelaybasedonyearofmanufacture;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author pratik
 */
public class Pearson_Reducer extends Reducer<Pearson_KeyWritable,Pearson_ValueWritable,Text,DoubleWritable>{

    private Text resultKey = new Text();
    
    @Override
    protected void reduce(Pearson_KeyWritable key, Iterable<Pearson_ValueWritable> values, Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
           
        
        double x = 0.0d;
        double y = 0.0d;
        double xx = 0.0d;
        double yy = 0.0d;   
        double xy = 0.0d;
	double n = 0.0d;
        
        for(Pearson_ValueWritable val : values){
            x += val.getElement1();
            y += val.getElement2();
            xx += Math.pow(val.getElement1(), 2.0);
            yy += Math.pow(val.getElement2(), 2.0);
            xy += val.getElement1() * val.getElement2();
            n += 1.0d;
        }
        
        double correlationFactor = getPearsonCorrelationFactor(x,y,xx,yy,xy,n);
        resultKey.set("The pearson correlation factor is : ");
        
        context.write(resultKey, new DoubleWritable(correlationFactor));
    
    }
    
       public double getPearsonCorrelationFactor(double x, double y, double xx, double yy, double xy, double n){
   
        double numerator = xy - ((x * y) / n);
        double denominator1 = xx - (Math.pow(x, 2.0) / n);
        double denominator2 = yy - (Math.pow(y, 2.0) / n);
        double denominator = Math.sqrt(denominator1 * denominator2);
        double correlationFactor = numerator / denominator;  
        return correlationFactor;
   } 
    
    
    
}
