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
import reactor.core.scheduler.Schedulers;

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
        // Expose the catch-up multiplier so we can see when we're behind pace and bidding up.
        metrics.registerGauge("pacing.catchup", pacing::catchUpFactor);
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
        return creativeCache.getAll().collectList().<Optional<BidResponse>>flatMap(all -> {
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
                return noBid(record, noBidReason(all, withinCap), start);
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
                // Persist off the response path: the record is for reporting/durability, not
                // needed to answer this request, so don't make the bidder wait on the shared
                // R2DBC pool to reply. Fire-and-forget on boundedElastic with error logging.
                persistAsync(record);
                return Mono.just(Optional.of(response));
            });
        })
        // Any failure in the chain (Redis timeout, DB acquire stall, etc.) becomes a fast no-bid,
        // never a 5xx. The controller also bounds this, but attributing it here gives an accurate
        // no-bid reason and a clean Optional.empty() instead of a bubbled error.
        .timeout(Duration.ofMillis(properties.getTimeoutMs()))
        .onErrorResume(ex -> {
            log.warn("<< BID ERROR  id={} — {}: {}", request.requestId(),
                    ex.getClass().getSimpleName(), ex.getMessage());
            metrics.recordNoBid("internal_error");
            return Mono.just(Optional.empty());
        });
    }

    /**
     * Persist a bid record without blocking the response. Runs on boundedElastic (the R2DBC
     * save may queue for a pooled connection) and logs on failure — a dropped record only
     * costs reporting fidelity, never a returned bid.
     */
    private void persistAsync(BidRecord record) {
        bidRecordRepository.save(record)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        saved -> {},
                        err -> log.warn("Failed to persist bid record id={}: {}",
                                record.getRequestId(), err.getMessage()));
    }

    /** No-bid terminal: stamp the reason, record metrics + latency, persist, return empty. */
    private Mono<Optional<BidResponse>> noBid(BidRecord record, String reason, long start) {
        record.setNoBidReason(reason);
        metrics.recordNoBid(reason);
        finish(record, start);
        // Same as the bid path: persist off the response path so a slow DB write can't
        // delay the 204. All no-bid records are kept (StatsService's bid-rate and no-bid-reason
        // breakdowns depend on the full set), just written asynchronously.
        persistAsync(record);
        return Mono.just(Optional.empty());
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
     * precedence order (cap, then targeting). Only called when {@code matching} is empty, so the
     * funnel emptied at the cap or targeting stage — budget exhaustion is attributed inline at
     * the budget check, not here:
     * <ul>
     *   <li>{@code targeting_miss} — creatives passed the cap but none matched the request's targeting</li>
     *   <li>{@code floor_exceeds_max_bid} — creatives exist but the floor is above every one's cap</li>
     *   <li>{@code no_eligible_creative} — the catalog is empty</li>
     * </ul>
     */
    private String noBidReason(List<Creative> all, List<Creative> withinCap) {
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
     * Bid price = just enough to win the auction, NOT our full cap. We anchor on the market's
     * recent clearing price plus a small margin, treating the creative's cap only as a ceiling.
     *
     * <p>Why not bid the full cap: in a first-price auction you pay exactly what you bid, and the
     * market here clears close to the floor (competitors win bidding only ~1.05–1.20× floor). Bidding
     * our full $1.50–$5.00 cap would win auctions but grossly overpay, so our per-creative budget
     * buys far fewer total wins than a bidder that pays just above the clearing price. The goal is to
     * win the auctions worth winning without leaving money on the table.
     *
     * <p>The bid anchor, in order:
     * <ul>
     *   <li>Once we've seen enough wins ({@code minSamples}), the rolling average clearing price times
     *       a small {@code marketMultiplier} margin — bid just over what the market has been paying.</li>
     *   <li>Before that (cold start), a fixed {@code coldStartMultiplier} margin over the floor.</li>
     * </ul>
     * In both cases we keep at least the cold-start margin over the floor, and {@link #enforceConstraints}
     * clamps the result to strictly clear the floor and never exceed the creative's cap.
     */
    private double computeBidPrice(BidRequest request, Creative creative) {
        BidderProperties.Strategy s = properties.getStrategy();
        double floor = request.floorPrice();

        double bid;
        if (statsCache.getSampleCount() >= s.getMinSamples()) {
            // Bid just above the market's recent clearing price — enough to win, not the full cap.
            bid = statsCache.getRollingAverageWinPrice() * s.getMarketMultiplier();
        } else {
            // Cold start (no market signal yet): a small fixed margin over the floor.
            bid = floor * s.getColdStartMultiplier();
        }
        // Never bid below what it takes to clear the floor with our cold-start margin.
        bid = Math.max(bid, floor * s.getColdStartMultiplier());

        // Time-based catch-up: when cumulative spend lags the linear pace, scale the bid up toward
        // the cap so leftover budget is spent before the deadline instead of finishing under budget.
        // 1.0 when on/ahead of pace; enforceConstraints still clamps the result to the creative cap.
        bid *= pacing.catchUpFactor();

        // enforceConstraints caps this at the creative's max bid, so the cap is a ceiling, not a target.
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
        return getRemainingBudgets()
                .map(m -> m.values().stream().mapToDouble(Double::doubleValue).sum());
    }

    /**
     * Remaining budget per creative id, in one batched Redis round trip (MGET) instead of one
     * GET per creative — the gauge supplier and /api/budget both poll this, and N serial Redis
     * calls on the shared pool are what {@link #getRemainingBudgetSafe} was added to bound.
     */
    public Mono<Map<String, Double>> getRemainingBudgets() {
        return creativeCache.getAll()
                .map(Creative::getId)
                .collectList()
                .flatMap(ids -> statsCache.getRemainingBudgets(ids)
                        .map(m -> new LinkedHashMap<String, Double>(m)));
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
