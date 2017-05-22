/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joinflights;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class Route_Compositekey implements Writable,WritableComparable<Route_Compositekey>{

     String origin;
     String destination;
     String originCity;
     String originState;
     String destCity;
     String destState;
     String originLat;
     String originLong;
     String destLat;
     String destLong;
     
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(origin);
        d.writeUTF(destination);
        d.writeUTF(originCity);
        d.writeUTF(originState);
        d.writeUTF(destCity);
        d.writeUTF(destState);
        d.writeUTF(originLat);
        d.writeUTF(originLong);
        d.writeUTF(destLat);
        d.writeUTF(destLong);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        
        origin = di.readUTF();
        destination = di.readUTF();
        originCity = di.readUTF();
        originState = di.readUTF();
        destCity =  di.readUTF();
        destState = di.readUTF();
        originLat= di.readUTF();
        originLong  = di.readUTF();
        destLat= di.readUTF();
        destLong = di.readUTF();
    
    }

    @Override
    public int compareTo(Route_Compositekey o) {
        int result = origin.compareTo(o.origin);
        return result;
    }
    
}
