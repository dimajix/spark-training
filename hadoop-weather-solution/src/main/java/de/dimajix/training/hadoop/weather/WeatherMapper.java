package de.dimajix.training.hadoop.weather;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;


class WeatherMapper extends Mapper<LongWritable,Text,Text,WeatherData> {
    private final Text country = new Text();
    private final WeatherData data = new WeatherData();
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

        Character windSpeedQuality = row.charAt(69);
        String windSpeed = row.substring(65,69);

        String countryCode = countries.get(station);

        if (countryCode != null) {
            country.set(countries.get(station));

            data.validTemperature = airTemperatureQuality == '1';
            data.minTemperature = Float.valueOf(airTemperature) / 10.f;
            data.maxTemperature = Float.valueOf(airTemperature) / 10.f;

            data.validWind = windSpeedQuality == '1';
            data.minWind = Float.valueOf(windSpeed) / 10.f;
            data.maxWind = Float.valueOf(windSpeed) / 10.f;

            context.getCounter("Weather Mapper", "Valid Wind").increment(data.validWind ? 1 : 0);
            context.getCounter("Weather Mapper", "Valid Temperature").increment(data.validTemperature ? 1 : 0);

            context.write(country, data);
        }
    }
}