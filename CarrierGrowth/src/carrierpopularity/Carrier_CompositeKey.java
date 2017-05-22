/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package carrierpopularity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class Carrier_CompositeKey implements Writable,WritableComparable<Carrier_CompositeKey>{
    
    Integer year;
    String carrierName;

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getCarrierName() {
        return carrierName;
    }

    public void setCarrierName(String carrierName) {
        this.carrierName = carrierName;
    }
    
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        d.writeUTF(carrierName);
        d.writeInt(year);
    
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        carrierName = di.readUTF();
        year = di.readInt();
    }

    @Override
    public int compareTo(Carrier_CompositeKey o) {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      int result  = year.compareTo(o.year);
      if(result==0){
          result = carrierName.compareTo(o.carrierName);
      }
      return result;
    }
    
    @Override
    public String toString() {
      return (new StringBuilder().append(year).append("\t").append(carrierName).toString());
    }    
}
