package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);

    private final BidderProperties properties;
    private final CreativeCache creativeCache;
    private final BidRecordRepository bidRecordRepository;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidRecordRepository bidRecordRepository,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.bidRecordRepository = bidRecordRepository;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);
    }

    /**
     * getRemainingBudget() does a DB query plus one Redis call per creative — under
     * DB/Redis pool contention (e.g. remote backing services with WAN latency) it can
     * queue for a connection indefinitely. /actuator/prometheus has no timeout of its
     * own, so an unbounded gauge supplier here stalls the entire scrape response.
     * Bound it the same way /api/bid bounds biddingService.bid(), and fall back to the
     * last known value instead of blocking Prometheus forever.
     *
     * <p>Micrometer's Gauge contract takes a plain synchronous Supplier<Number>, polled by the
     * Prometheus scrape thread — there's no reactive variant, so this is the one sanctioned
     * .block() outside of startup/Kafka-listener boundaries elsewhere in this codebase.
     */
    private double getRemainingBudgetSafe() {
        try {
            Double value = getRemainingBudget()
                    .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                    .onErrorReturn(lastKnownBudget)
                    .block();
            lastKnownBudget = value;
            return value;
        } catch (Exception ex) {
            return lastKnownBudget;
        }
    }

    public Mono<Optional<BidResponse>> bid(BidRequest request) {
        long start = System.nanoTime();
        metrics.recordRequest();
        BidRecord record = buildRecord(request);

        // 1. Targeting match, 2. floor within the creative's max bid, 3. still has budget.
        //    getRemainingBudget() is async (Mono<Double>) so the budget check uses filterWhen.
        Flux<Creative> eligible = matchingCreatives(request, creativeCache.getAll())
                .filter(c -> c.isWithinMaxBid(request.floorPrice()))
                .filterWhen(c -> statsCache.getRemainingBudget(c.getId()).map(remaining -> remaining > 0));

        // Take the first eligible creative and bid on it; if none survive, no-bid.
        return eligible.next()
                .flatMap(creative -> {
                    double bidPrice = computeBidPrice(request);
                    record.setBidPrice(bidPrice);
                    record.setCreativeId(creative.getId());
                    metrics.recordBid();
                    // Let AuctionNoticeConsumer resolve this bid from memory instead of hitting the DB.
                    ownBidCache.record(request.requestId(), creative.getId(), bidPrice);
                    finish(record, start);
                    BidResponse response = new BidResponse(request.requestId(), bidPrice, toCreativeDto(creative));
                    return bidRecordRepository.save(record).thenReturn(Optional.of(response));
                })
                .switchIfEmpty(Mono.defer(() -> {
                    String reason = "no_eligible_creative";
                    record.setNoBidReason(reason);
                    metrics.recordNoBid(reason);
                    finish(record, start);
                    return bidRecordRepository.save(record).thenReturn(Optional.<BidResponse>empty());
                }));
    }

    /** Stamp our processing latency onto the record and the timer metric. */
    private void finish(BidRecord record, long startNanos) {
        int latencyMs = (int) ((System.nanoTime() - startNanos) / 1_000_000);
        record.setLatencyMs(latencyMs);
        metrics.recordLatency(latencyMs);
    }

    private double computeBidPrice(BidRequest request) {
        BidderProperties.Strategy s = properties.getStrategy();
        double floor = request.floorPrice();

        // Before we've seen enough wins to know the market, bid a fixed margin over floor
        // (cold start). Once we have samples, anchor on the rolling average clearing price.
        double bid;
        if (statsCache.getSampleCount() < s.getMinSamples()) {
            bid = floor * s.getColdStartMultiplier();
        } else {
            double market = statsCache.getRollingAverageWinPrice() * s.getMarketMultiplier();
            // Never bid below the floor, even if the recent market has been cheap.
            bid = Math.max(market, floor * s.getColdStartMultiplier());
        }

        // Guarantee the bid strictly clears the floor.
        return Math.max(bid, floor * 1.01);
    }

    /** Total remaining budget across all this bidder's creatives. */
    public Mono<Double> getRemainingBudget() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()))
                .reduce(0.0, Double::sum);
    }

    /** Remaining budget per creative id. */
    public Mono<Map<String, Double>> getRemainingBudgets() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()).map(budget -> Map.entry(c.getId(), budget)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new);
    }

    private Flux<Creative> matchingCreatives(BidRequest request, Flux<Creative> all) {
        return all.filter(c -> c.matches(
                        request.targeting().geo(),
                        request.targeting().deviceType(),
                        request.targeting().audienceSegment()));
    }

    private CreativeDto toCreativeDto(Creative creative) {
        return new CreativeDto(
                creative.getId(),
                creative.getName(),
                creative.getDescription(),
                creative.getImageUrl(),
                creative.getCallToAction(),
                splitCsv(creative.getAllowedGeos()),
                splitCsv(creative.getAllowedDevices()),
                splitCsv(creative.getAudienceSegments())
        );
    }

    private List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private BidRecord buildRecord(BidRequest request) {
        BidRecord record = new BidRecord();
        record.setRequestId(request.requestId());
        record.setFloorPrice(request.floorPrice());
        if (request.targeting() != null) {
            record.setGeo(request.targeting().geo());
            record.setDeviceType(request.targeting().deviceType());
            record.setAudienceSegment(request.targeting().audienceSegment());
        }
        return record;
    }
}
