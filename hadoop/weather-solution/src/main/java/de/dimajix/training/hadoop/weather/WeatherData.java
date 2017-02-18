package de.dimajix.training.hadoop.weather;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kaya on 21.11.15.
 */
public class WeatherData implements Writable {
    public boolean validTemperature;
    public float minTemperature;
    public float maxTemperature;
    public boolean validWind;
    public float minWind;
    public float maxWind;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(validTemperature);
        dataOutput.writeFloat(minTemperature);
        dataOutput.writeFloat(maxTemperature);
        dataOutput.writeBoolean(validWind);
        dataOutput.writeFloat(minWind);
        dataOutput.writeFloat(maxWind);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        validTemperature = dataInput.readBoolean();
        minTemperature = dataInput.readFloat();
        maxTemperature = dataInput.readFloat();

        validWind = dataInput.readBoolean();
        minWind = dataInput.readFloat();
        maxWind = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "MinT: " + minTemperature + " MaxT: " +maxTemperature + " MinW:" + minWind + " MaxW:" + maxWind;
    }
}
