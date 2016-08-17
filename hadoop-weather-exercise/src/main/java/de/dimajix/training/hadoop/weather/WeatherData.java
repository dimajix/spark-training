package de.dimajix.training.hadoop.weather;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kaya on 21.11.15.
 */
public class WeatherData implements Writable {
    public float minTemperature;
    public float maxTemperature;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(minTemperature);
        dataOutput.writeFloat(maxTemperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        minTemperature = dataInput.readFloat();
        maxTemperature = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "MinT: " + minTemperature + " MaxT: " +maxTemperature;
    }
}
