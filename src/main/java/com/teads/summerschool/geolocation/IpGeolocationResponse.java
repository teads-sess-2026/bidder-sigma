package com.teads.summerschool.geolocation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Subset of the ipgeolocation.io /ipgeo response we care about for targeting.
 * Unknown fields are ignored so the API can evolve without breaking deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IpGeolocationResponse(
        @JsonProperty("ip") String ip,
        @JsonProperty("country_code2") String countryCode,
        @JsonProperty("country_name") String countryName,
        @JsonProperty("city") String city,
        @JsonProperty("continent_code") String continentCode
) {}
