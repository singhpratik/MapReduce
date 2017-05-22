/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package predictaveragedelay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class Predict_CompositeValue implements Writable,WritableComparable<Predict_CompositeValue>{

    
    private Integer count ;
    private Double Avereage;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getAvereage() {
        return Avereage;
    }

    public void setAvereage(Double Avereage) {
        this.Avereage = Avereage;
    }
    
    
    @Override
    public void write(DataOutput d) throws IOException {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      d.writeInt(count);
      d.writeDouble(Avereage);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
       count = di.readInt();
       Avereage = di.readDouble();
    }

    @Override
    public int compareTo(Predict_CompositeValue o) {
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    return 0;
    }

    @Override
    public String toString() {
        //return super.toString(); //To change body of generated methods, choose Tools | Templates.
         return (new StringBuilder().append(Avereage).toString());
    
    }
    
    
    
    
}
