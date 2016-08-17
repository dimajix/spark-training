package de.dimajix.training.hadoop.weather;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


class WeatherMapper extends Mapper<LongWritable,Text,Text,FloatWritable> {
    private Text country = new Text();
    private FloatWritable temperature = new FloatWritable();
    private final HashMap<String,String> countries = new HashMap<String,String>();

    @Override
    public void setup(Context context) {
        File countriesFile = new File("./countries");
        try (BufferedReader br = new BufferedReader(new FileReader(countriesFile))) {
            CSVParser parser = new CSVParser(br, CSVFormat.RFC4180);
            for (CSVRecord record : parser) {
                // USAF code of station
                String usaf = record.get(0);
                // WBAN code of station
                String wban = record.get(1);
                // Country code
                String country = record.get(3);

                String stationCcode = usaf + wban;
                countries.put(stationCcode, country);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup(Context context) {
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String row = value.toString();

        String station = row.substring(4,15);
        Character airTemperatureQuality = row.charAt(92);
        String airTemperature = row.substring(87,92);

        // Lookup country
        String countryCode = countries.get(station);

        // Only emit if quality is okay
        if (countryCode != null && airTemperatureQuality.charValue() == '1') {
            country.set(countryCode);
            temperature.set(Float.valueOf(airTemperature) / 10.f);
            context.write(country, temperature);
        }
    }
}
