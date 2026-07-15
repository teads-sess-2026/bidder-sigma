package com.teads.summerschool.geolocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Reactive client for ipgeolocation.io. Resolves an IP address to its geo attributes
 * so bids can be targeted by country/continent when the incoming request lacks a geo.
 *
 * <p>Non-blocking to fit the WebFlux bid path; every lookup is bounded by
 * {@link IpGeolocationProperties#getTimeoutMs()} and falls back to empty on error so a
 * slow or failing geolocation service can never block or fail a bid.
 */
@Component
public class IpGeolocationClient {

    private static final Logger log = LoggerFactory.getLogger(IpGeolocationClient.class);

    private final WebClient webClient;
    private final IpGeolocationProperties properties;

    public IpGeolocationClient(IpGeolocationProperties properties) {
        this.properties = properties;
        // Build directly via the static factory — this app doesn't expose a
        // WebClient.Builder bean, so don't depend on one being injectable.
        this.webClient = WebClient.builder().baseUrl(properties.getBaseUrl()).build();
    }

    /**
     * Look up geolocation for the given IP. Returns an empty Mono (not an error) when the
     * API key is unset, the IP is blank, or the call times out / fails.
     */
    public Mono<IpGeolocationResponse> lookup(String ip) {
        if (properties.getApiKey() == null || properties.getApiKey().isBlank() || ip == null || ip.isBlank()) {
            return Mono.empty();
        }
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/ipgeo")
                        .queryParam("apiKey", properties.getApiKey())
                        .queryParam("ip", ip)
                        .build())
                .retrieve()
                .bodyToMono(IpGeolocationResponse.class)
                .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                .onErrorResume(ex -> {
                    log.debug("ipgeolocation lookup failed for {}: {}", ip, ex.toString());
                    return Mono.empty();
                });
    }
}
