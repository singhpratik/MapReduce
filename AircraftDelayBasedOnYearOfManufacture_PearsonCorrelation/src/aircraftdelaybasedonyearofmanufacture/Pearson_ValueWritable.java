/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aircraftdelaybasedonyearofmanufacture;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class Pearson_ValueWritable implements Writable,WritableComparable<Pearson_ValueWritable>{

    private Double element1;
    private Double element2;

    public Double getElement1() {
        return element1;
    }

    public void setElement1(Double element1) {
        this.element1 = element1;
    }

    public Double getElement2() {
        return element2;
    }

    public void setElement2(Double element2) {
        this.element2 = element2;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      d.writeDouble(element1);
      d.writeDouble(element2);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        element1 = di.readDouble();
        element2 = di.readDouble();
    }

    @Override
    public int compareTo(Pearson_ValueWritable o) {
      int result = element1.compareTo(o.element1);
        if(result == 0){
            result = element2.compareTo(o.element2);
        }
        return result;
    }
    
      @Override
    public String toString() {
        //return super.toString(); //To change body of generated methods, choose Tools | Templates.
        return (new StringBuilder().append(element1).append("\t").append(element2).toString());
    
    }

}
