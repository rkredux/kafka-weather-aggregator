package com.github.rkredux.weather.producer;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.weather.example.Weather;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;

public class WeatherProducerDriver {

    public static void main(String[] args) {
        String appId = "redacted";
        String url = "https://api.openweathermap.org/data/2.5/weather";
        HttpResponse<JsonNode> jsonResponse = null;
        try {
            jsonResponse = Unirest.get(url)
                    .queryString("q", "london")
                    .queryString("appid", appId)
                    .asJson();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        JSONObject weatherJson = jsonResponse.getBody().getObject();

        long observationDateTimeUnix = weatherJson.getLong("dt");

        String location = weatherJson.getString("name");
        String locationId = weatherJson.getString("id");
        String country = weatherJson.getJSONObject("sys").getString("country");
        double latitude = weatherJson.getJSONObject("coord").getDouble("lat");
        double longitude = weatherJson.getJSONObject("coord").getDouble("lon");

        double temp = weatherJson.getJSONObject("main").getDouble("temp");
        double feelsLikeTemp = weatherJson.getJSONObject("main").getDouble("feels_like");
        double minimumTemp = weatherJson.getJSONObject("main").getDouble("temp_min");
        double maximumTemp = weatherJson.getJSONObject("main").getDouble("temp_max");
        double pressure = weatherJson.getJSONObject("main").getDouble("pressure");
        double humidity = weatherJson.getJSONObject("main").getDouble("pressure");


        String bootstrapServers = "127.0.0.1:9092";
        String schemaRegistryUrl = "http://127.0.0.1:8081";
        String topic = "weather-avro";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "KafkaAvroProducer");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl );

        KafkaProducer<String, Weather> kafkaProducer = new KafkaProducer<>(properties);

        Weather.Builder weatherBuilder = Weather.newBuilder();

        weatherBuilder.setLocation(location)
                .setLocationId(locationId)
                .setCountry(country)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setTemp(temp)
                .setFeelsLikeTemp(feelsLikeTemp)
                .setMinimumTemp(minimumTemp)
                .setMaximumTemp(maximumTemp)
                .setPressure(pressure)
                .setHumidity(humidity);

       Weather weather = weatherBuilder.build();
       ProducerRecord<String, Weather> weatherRecord = new ProducerRecord<String, Weather>(topic,weather);
        kafkaProducer.send(weatherRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println(recordMetadata.offset());
            } else e.printStackTrace();
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}




