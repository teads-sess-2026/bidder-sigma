package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Lookup for this bidder's creative catalog, served from an in-memory snapshot.
 *
 * <p>Originally read straight from Postgres on every bid() to stay correct, since creatives
 * can be added/removed after startup. But findByBidderId() runs on the bid hot path — a
 * ~200-row SELECT per request — and historically that table was also written by every
 * Kafka-confirmed win (recordWin used to sync creatives.budget), so it bloated under MVCC and
 * the scan slowed over a run. On the shared, size-capped R2DBC pool that read both queues for a
 * connection and blows the bid deadline, surfacing as 204 no-bids near the end of a run.
 *
 * <p>The catalog's biddable attributes (targeting, maxBidPrice, metadata) are immutable during
 * a run; remaining budget lives in Redis (owned and decremented by the SSP), and the bid path
 * reads it from there, not from the cached Creative. So a snapshot is safe. It's refreshed on the write path ({@link #refresh()},
 * called by CreativeSeeder) and, as a safety net for creatives added/removed externally on the
 * shared Postgres mid-run, re-loaded by a low-frequency background poll — invalidation by the
 * write path plus a slow poll, not a per-request read.
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;

    // The current catalog snapshot. Volatile: written by refresh()/the scheduled poll,
    // read by every bid() off the Netty event loop — publication must be visible across threads.
    private volatile List<Creative> snapshot = List.of();

    public CreativeCache(CreativeRepository repository, BidderProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    /** Serve the catalog from the in-memory snapshot — no Postgres round trip on the bid path. */
    public Flux<Creative> getAll() {
        return Flux.fromIterable(snapshot);
    }

    /**
     * Reload the snapshot from Postgres. Called by CreativeSeeder after seeding (the write path)
     * and by the scheduled safety-net poll. Returns after the snapshot has been swapped.
     */
    public Mono<Void> refresh() {
        return repository.findByBidderId(properties.getId())
                .collectList()
                .doOnNext(loaded -> {
                    this.snapshot = loaded;
                    log.info("Creative catalog snapshot refreshed: {} creatives", loaded.size());
                })
                .then();
    }

    /**
     * Safety net for creatives added/removed externally on the shared Postgres mid-run — the one
     * case the write-path refresh can't observe. Runs off the bid hot path, so the stale window is
     * bounded by the poll interval without any per-request DB cost. Errors are swallowed so a
     * transient DB blip just retries on the next tick rather than killing the scheduler thread.
     */
    @Scheduled(fixedDelayString = "${bidder.creative-cache.refresh-ms:30000}")
    void scheduledRefresh() {
        refresh().onErrorResume(e -> {
            log.warn("Scheduled creative catalog refresh failed, keeping previous snapshot: {}", e.getMessage());
            return Mono.empty();
        }).subscribe();
    }
}
