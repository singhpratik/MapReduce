/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagedelaybymonthbyyear;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
/**
 *
 * @author pratik
 */
public class CompositeValue_AverageDelay implements Writable{

    private Integer count;
    private Double averageDelay;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getAverageDelay() {
        return averageDelay;
    }

    public void setAverageDelay(Double averageDelay) {
        this.averageDelay = averageDelay;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
    //    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        d.writeInt(count);
        d.writeDouble(averageDelay);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
   //     throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        count = di.readInt();
        averageDelay = di.readDouble();
    }

    @Override
    public String toString() {
       // return super.toString(); //To change body of generated methods, choose Tools | Templates.
         return (new StringBuilder().append(averageDelay).toString());
    }
    
    
}
