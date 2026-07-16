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
    private final PacingController pacing;

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidRecordRepository bidRecordRepository,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache,
                          PacingController pacing) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.bidRecordRepository = bidRecordRepository;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
        this.pacing = pacing;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);
        // Expose the pacing multiplier so we can watch the controller react during a run.
        metrics.registerGauge("pacing.lambda", pacing::getLambda);
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

        // Funnel the catalog through the eligibility filters in the order F3 mandates —
        // 1. floor within the creative's max bid (checked before targeting and budget),
        // 2. targeting match, 3. still has budget (a single batched Redis MGET below). Keeping
        // the survivors of each stage lets us attribute an accurate no_bid_reason: whichever
        // stage emptied the funnel is the reason we didn't bid.
        return creativeCache.getAll().collectList().flatMap(all -> {
            List<Creative> withinCap = all.stream()
                    .filter(c -> c.isWithinMaxBid(request.floorPrice()))
                    .toList();
            List<Creative> matching = withinCap.stream()
                    .filter(c -> c.matches(
                            request.targeting().geo(),
                            request.targeting().deviceType(),
                            request.targeting().audienceSegment()))
                    .toList();

            if (matching.isEmpty()) {
                return noBid(record, noBidReason(all, withinCap, matching), start);
            }

            // Budget check for ALL matching creatives in a single Redis round trip (MGET), not
            // one sequential get per creative — N serial calls on the shared Lettuce pool blow
            // the ~50ms Redis / ~300ms bid deadline and surface as timeouts (204 no-bid).
            List<String> ids = matching.stream().map(Creative::getId).toList();
            return statsCache.getRemainingBudgets(ids).flatMap(budgets -> {
                List<Creative> eligible = matching.stream()
                        .filter(c -> budgets.getOrDefault(c.getId(), 0.0) > 0)
                        .toList();
                if (eligible.isEmpty()) {
                    return noBid(record, "budget_exhausted", start);
                }

                // Pick the most specific creative (tie-break on the highest cap) rather than
                // whichever the catalog happened to list first — a more targeted creative is
                // the better match for this impression.
                Creative creative = selectBestCreative(eligible);
                double bidPrice = computeBidPrice(request, creative);

                // Only bid if the money left on this creative can actually cover the bid.
                // budget > 0 above just means "some money left"; here we make sure it's
                // enough to pay the price we're about to commit to, so a nearly-exhausted
                // creative can't win an impression it can't afford and overspend its budget.
                double remaining = budgets.getOrDefault(creative.getId(), 0.0);
                if (bidPrice > remaining) {
                    return noBid(record, "budget_below_bid", start);
                }

                record.setBidPrice(bidPrice);
                record.setCreativeId(creative.getId());
                metrics.recordBid();
                pacing.recordParticipation(all.size());
                // Let AuctionNoticeConsumer resolve this bid from memory instead of hitting the DB.
                ownBidCache.record(request.requestId(), creative.getId(), bidPrice);
                finish(record, start);
                BidResponse response = new BidResponse(request.requestId(), bidPrice, toCreativeDto(creative));
                return bidRecordRepository.save(record).thenReturn(Optional.of(response));
            });
        });
    }

    /** No-bid terminal: stamp the reason, record metrics + latency, persist, return empty. */
    private Mono<Optional<BidResponse>> noBid(BidRecord record, String reason, long start) {
        record.setNoBidReason(reason);
        metrics.recordNoBid(reason);
        finish(record, start);
        return bidRecordRepository.save(record).thenReturn(Optional.<BidResponse>empty());
    }

    /**
     * Best creative among the budget-eligible survivors: most targeting dimensions constrained
     * (higher specificity = better match for this impression), tie-broken by the highest cap.
     */
    private Creative selectBestCreative(List<Creative> creatives) {
        return creatives.stream()
                .max((a, b) -> {
                    int specA = specificity(a);
                    int specB = specificity(b);
                    if (specA != specB) return Integer.compare(specA, specB);
                    double capA = a.getMaxBidPrice() != null ? a.getMaxBidPrice() : 0.0;
                    double capB = b.getMaxBidPrice() != null ? b.getMaxBidPrice() : 0.0;
                    return Double.compare(capA, capB);
                })
                .orElseThrow(() -> new IllegalStateException("selectBestCreative called with empty list"));
    }

    /** Count of constrained targeting dimensions (0-3); higher = more specifically targeted. */
    private int specificity(Creative c) {
        int score = 0;
        if (c.getAllowedGeos() != null && !c.getAllowedGeos().isBlank()) score++;
        if (c.getAllowedDevices() != null && !c.getAllowedDevices().isBlank()) score++;
        if (c.getAudienceSegments() != null && !c.getAudienceSegments().isBlank()) score++;
        return score;
    }

    /**
     * Attribute a no-bid to the eligibility stage that emptied the funnel, checked in F3's
     * precedence order (cap, then targeting, then budget):
     * <ul>
     *   <li>{@code budget_exhausted} — creatives matched cap and targeting but all are out of budget</li>
     *   <li>{@code targeting_miss} — creatives passed the cap but none matched the request's targeting</li>
     *   <li>{@code floor_exceeds_max_bid} — creatives exist but the floor is above every one's cap</li>
     *   <li>{@code no_eligible_creative} — the catalog is empty</li>
     * </ul>
     */
    private String noBidReason(List<Creative> all, List<Creative> withinCap, List<Creative> matching) {
        if (!matching.isEmpty()) return "budget_exhausted";
        if (!withinCap.isEmpty()) return "targeting_miss";
        if (!all.isEmpty()) return "floor_exceeds_max_bid";
        return "no_eligible_creative";
    }

    /** Stamp our processing latency onto the record and the timer metric. */
    private void finish(BidRecord record, long startNanos) {
        int latencyMs = (int) ((System.nanoTime() - startNanos) / 1_000_000);
        record.setLatencyMs(latencyMs);
        metrics.recordLatency(latencyMs);
    }

    /**
     * Bid price = per-impression <em>value</em>, shaded by the adaptive pacing multiplier λ, then
     * constrained to clear the floor and respect the creative's cap.
     *
     * <p>Following Gaitonde et al.'s adaptive-pacing algorithm, the pacing-aware bid is
     * {@code value / (1 + λ)}: bid true value when unconstrained (λ=0), shade proportionally as the
     * dual variable rises to keep spend on budget. λ is stepped in {@link PacingController#onOutcome}
     * off every observed win/loss — see {@link PacingController} and AuctionNoticeConsumer.
     *
     * <p>The value anchor is the creative's declared willingness-to-pay (its max cap) when set,
     * otherwise the rolling market clearing price once we've seen enough wins, otherwise a fixed
     * cold-start margin over the floor.
     */
    private double computeBidPrice(BidRequest request, Creative creative) {
        BidderProperties.Strategy s = properties.getStrategy();
        double floor = request.floorPrice();

        double value;
        if (creative.getMaxBidPrice() != null) {
            value = creative.getMaxBidPrice();
        } else if (statsCache.getSampleCount() >= s.getMinSamples()) {
            value = statsCache.getRollingAverageWinPrice() * s.getMarketMultiplier();
        } else {
            value = floor * s.getColdStartMultiplier();
        }
        // Value should never be below what it costs to clear the floor.
        value = Math.max(value, floor * s.getColdStartMultiplier());

        // Adaptive pacing: shade value by the current dual multiplier λ.
        double bid = value * pacing.shadeFactor();

        return enforceConstraints(bid, floor, creative);
    }

    /** Clamp a raw bid: strictly clear the floor, never exceed the creative's cap. */
    private double enforceConstraints(double bid, double floor, Creative creative) {
        bid = Math.max(bid, floor * 1.01);
        if (creative.getMaxBidPrice() != null) {
            bid = Math.min(bid, creative.getMaxBidPrice());
        }
        return bid;
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
