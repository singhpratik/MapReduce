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
public class CompositeKey_AverageDelay implements Writable,WritableComparable<CompositeKey_AverageDelay>{

    private Integer year;
    private String month;

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }
    
    
    @Override
    public void write(DataOutput d) throws IOException {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.  
     d.writeInt(year);
     d.writeUTF(month);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
    //    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        year = di.readInt();
        month = di.readUTF();
    }

    @Override
    public int compareTo(CompositeKey_AverageDelay o) {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      
      int result = -(1)*year.compareTo(o.year);
      
      if(result ==0){
          result = month.compareTo(o.month);
      }
      
      return result;
    }  

    @Override
    public String toString() {
       // return "CompositeKey_AverageDelay{" + "year=" + year + ", month=" + month + '}';
    
       return (new StringBuilder().append(year).append("\t").append(month).toString());
  
    }
    
    
}
