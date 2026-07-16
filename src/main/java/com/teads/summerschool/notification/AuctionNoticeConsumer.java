package com.teads.summerschool.notification;

import com.teads.summerschool.bidding.PacingController;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuctionNoticeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionNoticeConsumer.class);

    private final WinNoticeRepository winNoticeRepository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;
    private final PacingController pacing;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache,
                                 PacingController pacing) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
        this.pacing = pacing;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        try {
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);

            // This topic broadcasts EVERY auction's outcome to EVERY bidder, so most
            // messages a bidder receives are ones it never bid on. Filter on the
            // in-memory OwnBidCache (see BiddingService.bid()) BEFORE touching Redis or
            // Postgres — an O(1) local lookup instead of a DB round trip on every message.
            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());
            if (ourBid == null) {
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());

            log.info("KAFKA  id={} winner={} won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                double clearingPrice = notice.getClearingPrice();

                // Decrement the winning creative's remaining budget in Redis (and sync
                // Postgres's creatives.budget). Blocking is fine here — this Kafka consumer
                // thread is not the Netty event loop.
                statsCache.recordWin(ourBid.creativeId(), clearingPrice).block();

                // Persist the win for reporting / durability.
                winNoticeRepository.save(
                        new WinNotice(notice.getRequestId(), properties.getId(),
                                clearingPrice, ourBid.bidPrice())).block();

                metrics.recordWin(clearingPrice);

                // Adaptive pacing: we spent `clearingPrice` this auction — step λ toward budget.
                pacing.onOutcome(clearingPrice);

                log.info("** WIN  id={} creative={} clearing={} bid={}",
                        notice.getRequestId(), ourBid.creativeId(), clearingPrice, ourBid.bidPrice());
            } else {
                metrics.recordLoss();

                // Learn from the loss: the winner paid notice.getClearingPrice() — the level we
                // failed to beat. Feed it into the market window so our bid anchor climbs back up
                // when competitors move above us, instead of staying frozen at the cheap prices we
                // won early. (recordMarketPrice clamps fat-finger outliers.)
                statsCache.recordMarketPrice(notice.getClearingPrice());

                // Adaptive pacing: we paid nothing this auction (lost) — step λ down so we bid
                // more aggressively next time.
                pacing.onOutcome(0.0);

                log.info("** LOSS id={} creative={} bid={} clearing={}",
                        notice.getRequestId(), ourBid.creativeId(), ourBid.bidPrice(), notice.getClearingPrice());
            }
        } catch (Exception e) {
            log.error("** KAFKA ERROR  failed to process auction notice: {}", e.getMessage());
        }
    }
}
