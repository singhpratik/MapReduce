/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package totalflightandaveragebystate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class CompositeValue_State implements Writable{

    private Integer count;
    private Double arrivalDelay;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getArrivalDelay() {
        return arrivalDelay;
    }

    public void setArrivalDelay(Double arrivalDelay) {
        this.arrivalDelay = arrivalDelay;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        d.writeInt(count);
        d.writeDouble(arrivalDelay);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
       count = di.readInt();
       arrivalDelay = di.readDouble();
    }   

    @Override
    public String toString() {
      //  return super.toString(); //To change body of generated methods, choose Tools | Templates.
      return (new StringBuilder().append(arrivalDelay).append("\t").append(count).toString());
      
    }
    
}
