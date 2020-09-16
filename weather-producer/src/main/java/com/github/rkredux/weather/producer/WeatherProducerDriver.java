package com.github.rkredux.weather.producer;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class WeatherProducerDriver {

    WeatherProducerDriver(){}

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
        System.out.println(jsonResponse.getBody().getObject());
    }
}
