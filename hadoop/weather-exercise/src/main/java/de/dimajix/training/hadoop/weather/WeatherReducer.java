package de.dimajix.training.hadoop.weather;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


class WeatherReducer extends Reducer<Text,WeatherData,Text,WeatherData> {
    private WeatherData result = new WeatherData();

    @Override
    public void setup(Context context) {
    }

    @Override
    public void cleanup(Context context) {
    }

    @Override
    public void reduce(Text key, Iterable<WeatherData> values, Context context)
            throws IOException, InterruptedException {
    }
}
