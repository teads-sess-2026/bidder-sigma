package com.teads.summerschool.record;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-creative budget cache backed by Redis.
 *
 * <p>Key format: {@code {bidderId}_{creativeId}_budget}, value = remaining budget.
 * The SSP is the single owner of budget spend: it atomically decrements these keys on
 * each win. This bidder never writes spend — it only seeds keys once with SETNX
 * ({@code setIfAbsent}, so a bidder restart can't refill an already-spent budget) and
 * reads them to decide whether a creative can still spend.
 */
@Component
public class BidderStatsCache {

    private static final Logger log = LoggerFactory.getLogger(BidderStatsCache.class);

    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;

    private final AtomicLong winCount = new AtomicLong(0);
    // Total auctions we've observed a clearing price for (wins + losses). Used as the sample count
    // so the anchor switches from cold-start to the market average once we've seen enough of EITHER.
    private final AtomicLong observationCount = new AtomicLong(0);
    // Rolling window of observed clearing prices, fed by BOTH wins (what we paid) and losses (what
    // the winner paid). Anchoring on this de-biased window lets the bid climb back when the market
    // moves above us, instead of staying frozen at the cheap prices we won early.
    private final Deque<Double> recentWinPrices = new ArrayDeque<>();

    public BidderStatsCache(BidderProperties properties, ReactiveRedisTemplate<String, String> redis) {
        this.properties = properties;
        this.redis = redis;
    }

    /** Redis key holding the remaining budget for one creative. */
    public String budgetKey(String creativeId) {
        return properties.getId() + "_" + creativeId + "_budget";
    }

    /**
     * Seed a creative's remaining budget with its full limit, only if the key doesn't exist yet
     * (SETNX). The SSP owns spend on these keys, so a bidder restart must NOT refill an
     * already-spent budget. Called once per creative on startup.
     */
    public Mono<Boolean> initBudget(String creativeId, double budget) {
        String key = budgetKey(creativeId);
        return redis.opsForValue().setIfAbsent(key, String.valueOf(budget))
                .doOnNext(seeded -> {
                    if (Boolean.TRUE.equals(seeded)) {
                        log.info("Creative budget seeded: {} = {}", key, budget);
                    } else {
                        log.info("Creative budget already exists, left untouched: {}", key);
                    }
                });
    }

    /**
     * Record a Kafka-confirmed win in the local statistics (win count + rolling clearing-price
     * window). The budget key itself is NOT touched here — the SSP is the single owner of budget
     * spend and decrements it atomically on each win.
     */
    public void recordWin(String creativeId, double clearingPrice) {
        winCount.incrementAndGet();
        // A win records exactly what we paid — no outlier clamp needed (it's bounded
        // by our own bid, which is already clamped to the creative's cap).
        pushClearingPrice(clearingPrice);
    }

    /**
     * Feed the clearing price of an auction we LOST into the market window. A loss tells us the
     * winner paid {@code clearingPrice} — the level we failed to beat — so recording it lets the
     * anchor climb back up when the market moves above us, instead of staying frozen at the cheap
     * prices we won early (survivorship bias).
     *
     * <p>Guard against outliers: in a first-price auction the clearing price is the winner's raw
     * bid, so a single fat-finger overbid could be many times the real market level. Anything above
     * {@code average × marketOutlierMultiplier} is clamped to that ceiling before entering the
     * window, so one crazy-high winning bid can't drag our anchor up and make us overpay too. Before
     * we have any samples there's nothing to compare against, so the first prices are taken as-is.
     */
    public void recordMarketPrice(double clearingPrice) {
        if (clearingPrice <= 0) return;
        double avg = getRollingAverageWinPrice();
        double capped = clearingPrice;
        if (avg > 0) {
            double ceiling = avg * properties.getStrategy().getMarketOutlierMultiplier();
            if (capped > ceiling) {
                log.debug("MARKET  clamping outlier loss price {} to ceiling {}", clearingPrice, ceiling);
                capped = ceiling;
            }
        }
        pushClearingPrice(capped);
    }

    /** Append one price to the rolling market window, trimming to the configured window size. */
    private void pushClearingPrice(double price) {
        observationCount.incrementAndGet();
        synchronized (recentWinPrices) {
            recentWinPrices.addLast(price);
            if (recentWinPrices.size() > properties.getStrategy().getWindowSize()) {
                recentWinPrices.pollFirst();
            }
        }
    }

    /**
     * Remaining budget for several creatives in ONE Redis round trip (MGET), instead of a
     * sequential get-per-creative. The bid hot path checks every eligible creative's budget
     * under a tight (~50ms) Redis timeout inside an even tighter (~300ms) bid deadline; N
     * sequential round trips on the shared Lettuce pool blow that budget and surface as
     * RedisSystemException/timeouts (→ 204 no-bid). MGET makes it a single call regardless of N.
     *
     * <p>A missing key reads back as null here and is treated as the default creative budget
     * (matching {@link #getRemainingBudget}'s lazy-init semantics), but WITHOUT writing it back —
     * the next {@link #getRemainingBudget} call will lazy-seed it via setIfAbsent. Returns a map
     * keyed by creativeId in the same order as the input.
     */
    public Mono<java.util.Map<String, Double>> getRemainingBudgets(List<String> creativeIds) {
        if (creativeIds.isEmpty()) {
            return Mono.just(java.util.Map.of());
        }
        double defaultBudget = properties.getCreativeBudget();
        List<String> keys = creativeIds.stream().map(this::budgetKey).toList();
        return redis.opsForValue().multiGet(keys)
                .map(values -> {
                    java.util.Map<String, Double> out = new java.util.LinkedHashMap<>();
                    for (int i = 0; i < creativeIds.size(); i++) {
                        String raw = i < values.size() ? values.get(i) : null;
                        double budget = defaultBudget;
                        if (raw != null) {
                            try {
                                budget = Double.parseDouble(raw);
                            } catch (NumberFormatException e) {
                                budget = defaultBudget;
                            }
                        }
                        out.put(creativeIds.get(i), budget);
                    }
                    return out;
                });
    }

    /** Remaining budget for a creative. Lazily initializes to the flat creative budget if missing. */
    public Mono<Double> getRemainingBudget(String creativeId) {
        String key = budgetKey(creativeId);
        double defaultBudget = properties.getCreativeBudget();
        return redis.opsForValue().get(key)
                .flatMap(val -> {
                    try {
                        return Mono.just(Double.parseDouble(val));
                    } catch (NumberFormatException e) {
                        return Mono.just(defaultBudget);
                    }
                })
                .switchIfEmpty(redis.opsForValue().setIfAbsent(key, String.valueOf(defaultBudget))
                        .thenReturn(defaultBudget));
    }

    public long getWinCount() {
        return winCount.get();
    }

    public double getRollingAverageWinPrice() {
        synchronized (recentWinPrices) {
            if (recentWinPrices.isEmpty()) return 0.0;
            return recentWinPrices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
    }

    public long getSampleCount() {
        return observationCount.get();
    }
}
