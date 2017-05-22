/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top10busycity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class Top10CompositeKeyWritable implements Writable,WritableComparable<Top10CompositeKeyWritable>{
    
    private String city;
    private Integer totalNumberOfFlights;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Integer getTotalNumberOfFlights() {
        return totalNumberOfFlights;
    }

    public void setTotalNumberOfFlights(Integer totalNumberOfFlights) {
        this.totalNumberOfFlights = totalNumberOfFlights;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        
        d.writeUTF(city);
        d.writeInt(totalNumberOfFlights);
    
    }

    @Override
    public void readFields(DataInput di) throws IOException {
    //    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.    
        city = di.readUTF();
        totalNumberOfFlights = di.readInt();
    }

 
    @Override
    public int compareTo(Top10CompositeKeyWritable o) {
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
           
        int result =(-1)*totalNumberOfFlights.compareTo(o.totalNumberOfFlights);
        
//        if(result==0){
//            result = lastName.compareTo(o.lastName);
//        }  
        return result;
    }

    @Override
    public String toString() {
        return  city + "    " + totalNumberOfFlights ;
    }
}
