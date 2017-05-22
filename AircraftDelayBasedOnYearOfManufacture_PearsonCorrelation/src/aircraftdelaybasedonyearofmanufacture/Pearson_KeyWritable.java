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
public class Pearson_KeyWritable implements Writable,WritableComparable<Pearson_KeyWritable>{

    private Integer rowIndex;
    private Integer columnindex;

    public Integer getRowIndex() {
        return rowIndex;
    }

    public void setRowIndex(Integer rowIndex) {
        this.rowIndex = rowIndex;
    }

    public Integer getColumnindex() {
        return columnindex;
    }

    public void setColumnindex(Integer columnindex) {
        this.columnindex = columnindex;
    }
    
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
       d.writeInt(rowIndex);
       d.writeInt(columnindex);
    
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
       rowIndex = di.readInt();
       columnindex = di.readInt();
    }

    @Override
    public int compareTo(Pearson_KeyWritable o) {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      
               int result = rowIndex.compareTo(o.rowIndex);
        if(result == 0){
            result = columnindex.compareTo(o.columnindex);
        }
        return result;
    
    }

    @Override
    public String toString() {
        //return super.toString(); //To change body of generated methods, choose Tools | Templates.
        return (new StringBuilder().append(rowIndex).append("\t").append(columnindex).toString());
    
    }

    
    
    
    
}
