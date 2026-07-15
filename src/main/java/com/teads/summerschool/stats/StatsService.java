package com.teads.summerschool.stats;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.notification.WinNotice;
import com.teads.summerschool.notification.WinNoticeRepository;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.stats.dto.CreativeStatsResponse;
import com.teads.summerschool.stats.dto.StatsResponse;
import com.teads.summerschool.stats.dto.TargetingResponse;
import com.teads.summerschool.stats.dto.TimeseriesResponse;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only dashboard aggregations over the two things this bidder persists: every auction it saw
 * (bid_record) and every win it was notified of (win_notice). Everything is computed reactively off
 * the existing R2DBC repositories — no extra DB wiring needed.
 *
 * <p>win_notice carries no creative/targeting columns, so wins are attributed back to a creative or
 * a targeting bucket by joining on request_id against bid_record, in memory. Data volume per
 * competition is small enough that loading the rows and aggregating here is simpler and cheaper than
 * a fan-out of grouped SQL queries.
 */
@Service
public class StatsService {

    private final BidderProperties properties;
    private final BidRecordRepository bidRecordRepository;
    private final WinNoticeRepository winNoticeRepository;
    private final CreativeCache creativeCache;

    public StatsService(BidderProperties properties,
                        BidRecordRepository bidRecordRepository,
                        WinNoticeRepository winNoticeRepository,
                        CreativeCache creativeCache) {
        this.properties = properties;
        this.bidRecordRepository = bidRecordRepository;
        this.winNoticeRepository = winNoticeRepository;
        this.creativeCache = creativeCache;
    }

    // ---------------------------------------------------------------------------------------------
    // Overall snapshot
    // ---------------------------------------------------------------------------------------------

    public Mono<StatsResponse> getStats() {
        return Mono.zip(
                bidRecordRepository.findAll().collectList(),
                winNoticeRepository.findAll().collectList()
        ).map(t -> buildStats(t.getT1(), t.getT2()));
    }

    private StatsResponse buildStats(List<BidRecord> records, List<WinNotice> wins) {
        LocalDateTime now = LocalDateTime.now();

        long totalAuctions = records.size();
        List<BidRecord> bidRecords = records.stream().filter(r -> r.getBidPrice() != null).toList();
        long bids = bidRecords.size();
        long noBids = totalAuctions - bids;
        long winCount = wins.size();

        double avgBidPrice = bidRecords.stream().mapToDouble(BidRecord::getBidPrice).average().orElse(0.0);
        double avgWinPrice = wins.stream().mapToDouble(WinNotice::getClearingPrice).average().orElse(0.0);
        double totalSpend = wins.stream().mapToDouble(WinNotice::getClearingPrice).sum();

        double budget = properties.getBudget();
        double remainingBudget = Math.max(0.0, budget - totalSpend);

        double bidRate = totalAuctions > 0 ? (double) bids / totalAuctions : 0.0;
        double winRate = bids > 0 ? (double) winCount / bids : 0.0;
        double winRatePerAuction = totalAuctions > 0 ? (double) winCount / totalAuctions : 0.0;

        return new StatsResponse(
                properties.getId(),
                now,
                totalAuctions, bids, noBids, bidRate,
                winCount, winRate, winRatePerAuction,
                avgBidPrice, avgWinPrice, totalSpend,
                remainingBudget, budget,
                latencyStats(records),
                noBidReasons(records),
                pacing(records, now, totalSpend, budget, remainingBudget)
        );
    }

    private StatsResponse.LatencyStats latencyStats(List<BidRecord> records) {
        List<Integer> sorted = records.stream()
                .map(BidRecord::getLatencyMs)
                .filter(l -> l != null)
                .sorted()
                .toList();
        if (sorted.isEmpty()) {
            return new StatsResponse.LatencyStats(0.0, 0, 0, 0, 0);
        }
        double avg = sorted.stream().mapToInt(Integer::intValue).average().orElse(0.0);
        return new StatsResponse.LatencyStats(
                avg,
                percentile(sorted, 0.50),
                percentile(sorted, 0.95),
                sorted.get(sorted.size() - 1),
                sorted.size()
        );
    }

    /** Nearest-rank percentile over an already-sorted, non-empty list. */
    private int percentile(List<Integer> sorted, double q) {
        int rank = (int) Math.ceil(q * sorted.size());
        int idx = Math.min(Math.max(rank, 1), sorted.size()) - 1;
        return sorted.get(idx);
    }

    private StatsResponse.NoBidReasons noBidReasons(List<BidRecord> records) {
        int budgetExhausted = 0, noEligibleCreative = 0, targetingMiss = 0;
        for (BidRecord r : records) {
            if (r.getNoBidReason() == null) continue;
            switch (r.getNoBidReason()) {
                case "budget_exhausted" -> budgetExhausted++;
                case "no_eligible_creative" -> noEligibleCreative++;
                case "targeting_miss" -> targetingMiss++;
                default -> { /* floor_exceeds_max_bid or unknown — not surfaced in this DTO */ }
            }
        }
        return new StatsResponse.NoBidReasons(budgetExhausted, noEligibleCreative, targetingMiss);
    }

    private StatsResponse.PacingStats pacing(List<BidRecord> records, LocalDateTime now,
                                             double totalSpend, double budget, double remainingBudget) {
        LocalDateTime first = records.stream()
                .map(BidRecord::getCreatedAt)
                .filter(t -> t != null)
                .min(Comparator.naturalOrder())
                .orElse(now);

        double elapsedMinutes = Math.max(0.0, ChronoUnit.MILLIS.between(first, now) / 60_000.0);
        double spendPerMinute = elapsedMinutes > 0 ? totalSpend / elapsedMinutes : 0.0;
        Double projected = spendPerMinute > 0 ? remainingBudget / spendPerMinute : null;
        double utilization = budget > 0 ? totalSpend / budget : 0.0;

        return new StatsResponse.PacingStats(spendPerMinute, elapsedMinutes, projected, utilization);
    }

    // ---------------------------------------------------------------------------------------------
    // Per-creative
    // ---------------------------------------------------------------------------------------------

    // Valid sort values: spend, wins, bids, bid_rate, win_rate — Valid order values: asc, desc
    public Mono<CreativeStatsResponse> getCreativeStats(String creativeId, String sort, String order) {
        return Mono.zip(
                bidRecordRepository.findAll().collectList(),
                winNoticeRepository.findAll().collectList(),
                creativeCache.getAll().collectMap(c -> c.getId(), c -> c.getName())
        ).map(t -> buildCreativeStats(t.getT1(), t.getT2(), t.getT3(), creativeId, sort, order));
    }

    private CreativeStatsResponse buildCreativeStats(List<BidRecord> records, List<WinNotice> wins,
                                                     Map<String, String> creativeNames,
                                                     String creativeId, String sort, String order) {
        // request_id -> creative_id, to attribute wins (which lack a creative column) back to a creative.
        Map<String, String> requestToCreative = new LinkedHashMap<>();
        Map<String, Agg> byCreative = new LinkedHashMap<>();

        for (BidRecord r : records) {
            if (r.getBidPrice() == null || r.getCreativeId() == null) continue;
            requestToCreative.put(r.getRequestId(), r.getCreativeId());
            Agg agg = byCreative.computeIfAbsent(r.getCreativeId(), k -> new Agg());
            agg.bids++;
            agg.bidSum += r.getBidPrice();
        }

        for (WinNotice w : wins) {
            String cId = requestToCreative.get(w.getRequestId());
            if (cId == null) continue; // win we can't attribute to one of our creatives
            Agg agg = byCreative.computeIfAbsent(cId, k -> new Agg());
            agg.wins++;
            agg.spend += w.getClearingPrice();
        }

        List<CreativeStatsResponse.CreativeStat> stats = new ArrayList<>();
        byCreative.forEach((id, agg) -> {
            if (creativeId != null && !creativeId.isBlank() && !creativeId.equals(id)) return;
            stats.add(new CreativeStatsResponse.CreativeStat(
                    id,
                    creativeNames.getOrDefault(id, id),
                    agg.bids,
                    agg.wins,
                    agg.bids > 0 ? (double) agg.wins / agg.bids : 0.0,
                    agg.bids > 0 ? agg.bidSum / agg.bids : 0.0,
                    agg.wins > 0 ? agg.spend / agg.wins : 0.0,
                    agg.spend
            ));
        });

        sortCreatives(stats, sort, order);
        return new CreativeStatsResponse(properties.getId(), stats);
    }

    private void sortCreatives(List<CreativeStatsResponse.CreativeStat> stats, String sort, String order) {
        Comparator<CreativeStatsResponse.CreativeStat> cmp = switch (sort) {
            case "wins"     -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::wins);
            case "bids"     -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::bids);
            case "win_rate" -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::winRate);
            // No per-creative "bid rate" (we don't track auctions-seen per creative); order by bid volume.
            case "bid_rate" -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::bids);
            default         -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::spend); // "spend"
        };
        if (!"asc".equals(order)) cmp = cmp.reversed();
        stats.sort(cmp);
    }

    // ---------------------------------------------------------------------------------------------
    // Targeting
    // ---------------------------------------------------------------------------------------------

    // Valid dimension values: geo, device, segment, all
    public Mono<TargetingResponse> getTargetingStats(String dimension) {
        return Mono.zip(
                bidRecordRepository.findAll().collectList(),
                winNoticeRepository.findAll().collectList()
        ).map(t -> buildTargeting(t.getT1(), t.getT2(), dimension));
    }

    private TargetingResponse buildTargeting(List<BidRecord> records, List<WinNotice> wins, String dimension) {
        boolean geo     = "all".equals(dimension) || "geo".equals(dimension);
        boolean device  = "all".equals(dimension) || "device".equals(dimension);
        boolean segment = "all".equals(dimension) || "segment".equals(dimension);

        // request_id -> targeting key, per dimension, so wins can be attributed back to a bucket.
        Map<String, String> reqToGeo = new LinkedHashMap<>();
        Map<String, String> reqToDevice = new LinkedHashMap<>();
        Map<String, String> reqToSegment = new LinkedHashMap<>();

        Map<String, Agg> byGeo = new LinkedHashMap<>();
        Map<String, Agg> byDevice = new LinkedHashMap<>();
        Map<String, Agg> bySegment = new LinkedHashMap<>();

        for (BidRecord r : records) {
            if (r.getBidPrice() == null) continue; // buckets describe bids we placed
            if (geo)     accumulateBid(byGeo, reqToGeo, r.getRequestId(), r.getGeo(), r.getBidPrice());
            if (device)  accumulateBid(byDevice, reqToDevice, r.getRequestId(), r.getDeviceType(), r.getBidPrice());
            if (segment) accumulateBid(bySegment, reqToSegment, r.getRequestId(), r.getAudienceSegment(), r.getBidPrice());
        }

        for (WinNotice w : wins) {
            if (geo)     accumulateWin(byGeo, reqToGeo.get(w.getRequestId()));
            if (device)  accumulateWin(byDevice, reqToDevice.get(w.getRequestId()));
            if (segment) accumulateWin(bySegment, reqToSegment.get(w.getRequestId()));
        }

        return new TargetingResponse(
                properties.getId(),
                toBuckets(byGeo),
                toBuckets(byDevice),
                toBuckets(bySegment)
        );
    }

    private void accumulateBid(Map<String, Agg> buckets, Map<String, String> reqToKey,
                               String requestId, String key, double bidPrice) {
        String k = key == null || key.isBlank() ? "unknown" : key;
        reqToKey.put(requestId, k);
        Agg agg = buckets.computeIfAbsent(k, x -> new Agg());
        agg.bids++;
        agg.bidSum += bidPrice;
    }

    private void accumulateWin(Map<String, Agg> buckets, String key) {
        if (key == null) return;
        buckets.computeIfAbsent(key, x -> new Agg()).wins++;
    }

    private List<TargetingResponse.TargetingBucket> toBuckets(Map<String, Agg> buckets) {
        List<TargetingResponse.TargetingBucket> out = new ArrayList<>();
        buckets.forEach((key, agg) -> out.add(new TargetingResponse.TargetingBucket(
                key,
                agg.bids,
                agg.wins,
                agg.bids > 0 ? (double) agg.wins / agg.bids : 0.0,
                agg.bids > 0 ? agg.bidSum / agg.bids : 0.0
        )));
        out.sort(Comparator.comparingLong(TargetingResponse.TargetingBucket::bids).reversed());
        return out;
    }

    // ---------------------------------------------------------------------------------------------
    // Timeseries
    // ---------------------------------------------------------------------------------------------

    // windowMinutes: clamp to [1, 180]; bucketSeconds: clamp to min 10
    public Mono<TimeseriesResponse> getTimeseries(int windowMinutes, int bucketSeconds) {
        int window = Math.max(1, Math.min(180, windowMinutes));
        int bucket = Math.max(10, bucketSeconds);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime since = now.minusMinutes(window);

        return Mono.zip(
                bidRecordRepository.findByCreatedAtAfter(since).collectList(),
                winNoticeRepository.findByReceivedAtAfter(since).collectList()
        ).map(t -> buildTimeseries(t.getT1(), t.getT2(), window, bucket, since));
    }

    private TimeseriesResponse buildTimeseries(List<BidRecord> records, List<WinNotice> wins,
                                               int window, int bucket, LocalDateTime since) {
        int numBuckets = (int) Math.ceil((window * 60.0) / bucket);
        Bucket[] buckets = new Bucket[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new Bucket(since.plusSeconds((long) i * bucket));
        }

        for (BidRecord r : records) {
            int i = bucketIndex(since, r.getCreatedAt(), bucket, numBuckets);
            if (i < 0) continue;
            buckets[i].auctions++;
            if (r.getBidPrice() != null) {
                buckets[i].bids++;
                buckets[i].bidSum += r.getBidPrice();
            }
        }
        for (WinNotice w : wins) {
            int i = bucketIndex(since, w.getReceivedAt(), bucket, numBuckets);
            if (i < 0) continue;
            buckets[i].wins++;
            buckets[i].spend += w.getClearingPrice();
        }

        List<TimeseriesResponse.Point> points = new ArrayList<>(numBuckets);
        for (Bucket b : buckets) {
            points.add(new TimeseriesResponse.Point(
                    b.time,
                    b.auctions,
                    b.bids,
                    b.wins,
                    b.auctions > 0 ? (double) b.bids / b.auctions : 0.0,
                    b.bids > 0 ? (double) b.wins / b.bids : 0.0,
                    b.bids > 0 ? b.bidSum / b.bids : 0.0,
                    b.spend
            ));
        }
        return new TimeseriesResponse(properties.getId(), window, bucket, points);
    }

    private int bucketIndex(LocalDateTime since, LocalDateTime t, int bucketSeconds, int numBuckets) {
        if (t == null || t.isBefore(since)) return -1;
        long secs = ChronoUnit.SECONDS.between(since, t);
        int idx = (int) (secs / bucketSeconds);
        return idx >= 0 && idx < numBuckets ? idx : -1;
    }

    /** Mutable per-group accumulator used across creative/targeting aggregation. */
    private static final class Agg {
        long bids;
        long wins;
        double bidSum;
        double spend;
    }

    /** Mutable per-time-bucket accumulator for the timeseries. */
    private static final class Bucket {
        final LocalDateTime time;
        long auctions;
        long bids;
        long wins;
        double bidSum;
        double spend;

        Bucket(LocalDateTime time) { this.time = time; }
    }
}
