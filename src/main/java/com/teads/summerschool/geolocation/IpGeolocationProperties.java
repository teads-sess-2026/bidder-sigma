package com.teads.summerschool.geolocation;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "ipgeolocation")
public class IpGeolocationProperties {

    private String apiKey = "";
    private String baseUrl = "https://api.ipgeolocation.io";
    // Bound the lookup well under the ~100ms whole-bid budget: a healthy lookup is 10-30ms,
    // and on timeout lookup() falls back to empty so a slow geo service never fails the bid.
    // Kept below Redis's 50ms budget so geo + budget checks together stay within budget.
    private long timeoutMs = 40;

    public String getApiKey() { return apiKey; }
    public void setApiKey(String apiKey) { this.apiKey = apiKey; }

    public String getBaseUrl() { return baseUrl; }
    public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

    public long getTimeoutMs() { return timeoutMs; }
    public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
}
