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
public class Predict_CompositeKey implements Writable,WritableComparable<Predict_CompositeKey>{
    
    private Integer flightNumber;
    private Integer dayOfMonth;
    private Integer month;

    public Integer getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(Integer flightNumber) {
        this.flightNumber = flightNumber;
    }

    public Integer getDayOfMonth() {
        return dayOfMonth;
    }
    
    public void setDayOfMonth(Integer dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }
 
    
    
    @Override
    public void write(DataOutput d) throws IOException {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      d.writeInt(flightNumber);
      d.writeInt(dayOfMonth);
      d.writeInt(month);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        
        flightNumber = di.readInt();
        dayOfMonth = di.readInt();
        month = di.readInt();
    
    }

    @Override
    public int compareTo(Predict_CompositeKey o) {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      
      int result = flightNumber.compareTo(o.flightNumber);
      if(result==0){
          result = (-1)*month.compareTo(o.month);
          if(result==0){
              result = (-1)*dayOfMonth.compareTo(o.dayOfMonth);
          }
      }
      return result;
    }
    
    @Override
    public String toString() {
      //  return "Predict_CompositeKey{" + "flightNumber=" + flightNumber + ", dayOfMonth=" + dayOfMonth + ", month=" + month + '}';
       return (new StringBuilder().append(flightNumber).append("\t").append(month).append("\t").append(dayOfMonth).toString());

    }
    
    
}
