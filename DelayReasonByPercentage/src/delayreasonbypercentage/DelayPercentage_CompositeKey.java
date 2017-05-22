/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package delayreasonbypercentage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author pratik
 */
public class DelayPercentage_CompositeKey implements Writable,WritableComparable<DelayPercentage_CompositeKey>{

    private Integer carrierDelay;
    private Integer weatherDelay;
    private Integer nasDelay;
    private Integer securityDelay;
    private Integer lateAirDelay;
    private Integer totalDelay;

    public Integer getTotalDelay() {
        return totalDelay;
    }

    public void setTotalDelay(Integer totalDelay) {
        this.totalDelay = totalDelay;
    }
    
    

    public Integer getCarrierDelay() {
        return carrierDelay;
    }

    public void setCarrierDelay(Integer carrierDelay) {
        this.carrierDelay = carrierDelay;
    }

    public Integer getWeatherDelay() {
        return weatherDelay;
    }

    public void setWeatherDelay(Integer weatherDelay) {
        this.weatherDelay = weatherDelay;
    }

    public Integer getNasDelay() {
        return nasDelay;
    }

    public void setNasDelay(Integer nasDelay) {
        this.nasDelay = nasDelay;
    }

    public Integer getSecurityDelay() {
        return securityDelay;
    }

    public void setSecurityDelay(Integer securityDelay) {
        this.securityDelay = securityDelay;
    }

    public Integer getLateAirDelay() {
        return lateAirDelay;
    }

    public void setLateAirDelay(Integer lateAirDelay) {
        this.lateAirDelay = lateAirDelay;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        d.writeInt(carrierDelay);
        d.writeInt(weatherDelay);
        d.writeInt(nasDelay);
        d.writeInt(securityDelay);
        d.writeInt(lateAirDelay);
        d.writeInt(totalDelay);
    
    }

    @Override
    public void readFields(DataInput di) throws IOException {
      //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        carrierDelay = di.readInt();
        weatherDelay = di.readInt();
        nasDelay = di.readInt();
        securityDelay = di.readInt();
        lateAirDelay = di.readInt();
        totalDelay = di.readInt();
        }

    @Override
    public int compareTo(DelayPercentage_CompositeKey o) {
      int result =0;
      return result;
      
    }

    @Override
    public String toString() {
         //To change body of generated methods, choose Tools | Templates.
          return (new StringBuilder().append(carrierDelay).append("\t").append(weatherDelay).append("\t").append(nasDelay).append("\t").append(securityDelay).append("\t").append(lateAirDelay).append("\t").append(totalDelay).toString());
    }
    
    
    
}
