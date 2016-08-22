package cep;

import java.io.Serializable;

public class TransportEvent implements Serializable {

    private static final long serialVersionUID = -8239659693120458106L;

    public String id, type, timestamp_created, latitude, longitude, altitude, accuracy, bearing, provider, speed, shipment_number, sensor_id, resolution, power_consumption,
            battery_percentage, battery_temperature, acceleration, brightness, orientation_fw_bw, orientation_l_r, temperature, pressure, relative_humidity;

    public TransportEvent() {}

    public TransportEvent(String id, String type, String timestamp_created,
                          String latitude, String longitude, String altitude,
                          String accuracy, String bearing, String provider,
                          String speed, String shipment_number, String sensor_id,
                          String resolution, String power_consumption, String battery_percentage,
                          String battery_temperature, String acceleration, String brightness,
                          String orientation_fw_bw, String orientation_l_r, String temperature,
                          String pressure, String relative_humidity) {

        this.id = id;
        this.type = type;
        this.timestamp_created = timestamp_created;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.accuracy = accuracy;
        this.bearing = bearing;
        this.provider = provider;
        this.speed = speed;
        this.shipment_number = shipment_number;
        this.sensor_id = sensor_id;
        this.resolution = resolution;
        this.power_consumption = power_consumption;
        this.battery_percentage = battery_percentage;
        this.battery_temperature = battery_temperature;
        this.acceleration = acceleration;
        this.brightness = brightness;
        this.orientation_fw_bw = orientation_fw_bw;
        this.orientation_l_r = orientation_l_r;
        this.temperature = temperature;
        this.pressure = pressure;
        this.relative_humidity = relative_humidity;
    }

}