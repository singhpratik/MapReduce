/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package busyroutes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class BusyRoute_CompositeKey implements Writable,WritableComparable<BusyRoute_CompositeKey>{
    
    private String origin;
    private String destination;

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
    //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        d.writeUTF(origin);
        d.writeUTF(destination);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
    //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
       origin = di.readUTF();
       destination = di.readUTF();
    }

    @Override
    public int compareTo(BusyRoute_CompositeKey o) {
     //   throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
     int result = origin.compareTo(o.origin);
     
        if(result==0){
            result = destination.compareTo(o.destination);
        }
        
     return result;
    }  

    @Override
    public String toString() {
         return (new StringBuilder().append(origin).append("\t").append(destination).toString());
        //return origin+"\\t"+destination ;//To change body of generated methods, choose Tools | Templates.
    }
}
