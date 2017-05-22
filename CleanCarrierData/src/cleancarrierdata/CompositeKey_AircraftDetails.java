/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cleancarrierdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class CompositeKey_AircraftDetails implements Writable,WritableComparable<CompositeKey_AircraftDetails>{

    private String tailNumber;
    private Integer year;
    private String aircraftType;

    public String getTailNumber() {
        return tailNumber;
    }

    public void setTailNumber(String tailNumber) {
        this.tailNumber = tailNumber;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getAircraftType() {
        return aircraftType;
    }

    public void setAircraftType(String aircraftType) {
        this.aircraftType = aircraftType;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      d.writeUTF(tailNumber);
      d.writeUTF(aircraftType);
      d.writeInt(year);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
       tailNumber = di.readUTF();
       aircraftType = di.readUTF();
       year = di.readInt();
    
    }

    @Override
    public int compareTo(CompositeKey_AircraftDetails o) {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      int result =(-1)*year.compareTo(year);
      return result;
    }

    @Override
    public String toString() {
        //return "CompositeKey_AircraftDetails{" + "tailNumber=" + tailNumber + ", year=" + year + ", aircraftType=" + aircraftType + '}';
    return (new StringBuilder().append(year).append("\t").append(tailNumber).append("\t").append(aircraftType).toString());
    }
    
    
    
}
