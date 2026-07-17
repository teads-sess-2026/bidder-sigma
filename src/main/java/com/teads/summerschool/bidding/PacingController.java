package com.teads.summerschool.bidding;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adaptive budget pacing, following the dual-gradient (adaptive pacing) algorithm analyzed in
 * Gaitonde et al., "Budget Pacing in Repeated Auctions: Regret and Efficiency without Convergence".
 *
 * <p>The idea: we hold a single pacing multiplier {@code λ ≥ 0} (a Lagrange/dual variable for the
 * budget constraint) and bid our per-impression value <em>shaded</em> by it:
 * <pre>{@code   bid = value / (1 + λ) }</pre>
 * After every auction we participated in, we observe our payment {@code z} (the clearing price if
 * we won, 0 if we lost) and take one projected gradient step toward a per-auction target spend
 * {@code ρ}:
 * <pre>{@code   λ ← clip( λ − η·(ρ − z), 0, λ_max ) }</pre>
 * Won and paid more than ρ → λ rises → we shade harder. Lost (paid 0) → λ falls → we bid more
 * aggressively next time. The paper's result is that this achieves low regret and high budget
 * efficiency <em>even when the market never converges</em> — i.e. when every competing bidder is
 * pacing too, which is exactly the competition setting.
 *
 * <p>ρ is the average budget we can afford to spend per auction we take part in. We don't know the
 * total auction count up front, so we estimate it live from our own participation rate over the
 * competition window: {@code ρ = B · elapsed / (participations · duration)}. As participations
 * accumulate this settles to {@code B / T}. If no competition start time is configured we lazily
 * start the clock on the first bid, so pacing is always self-starting.
 *
 * <p>All state is a couple of volatiles plus an AtomicLong; {@link #shadeFactor()} is called on the
 * Netty event loop (bid hot path) and {@link #onOutcome(double)} on the Kafka consumer thread, so
 * both are lock-free and cheap.
 */
@Component
public class PacingController {

    private static final Logger log = LoggerFactory.getLogger(PacingController.class);

    private final BidderProperties properties;

    // The dual variable λ. Starts at 0 (no shading) and is clipped to [0, λ_max].
    private volatile double lambda = 0.0;

    // Auctions we have actually bid in, used to estimate the total horizon T live.
    private final AtomicLong participations = new AtomicLong(0);

    // Actual spend so far, in cents (clearing price summed over every auction we won). Compared
    // against the linear-pace target in catchUpFactor to scale bids up when we fall behind. Cents
    // (long) so the running total is exact and lock-free — see onOutcome.
    private final AtomicLong spentCents = new AtomicLong(0);

    // Total budget B across all our creatives. Budget is tracked per creative (flat
    // creativeBudget each), so B = creativeBudget * catalog size. The catalog can change at
    // runtime, so BiddingService refreshes this from the size it already collected per bid.
    private volatile double totalBudget;

    // Competition start; parsed from config if set, else stamped on the first bid.
    private volatile Instant start;

    public PacingController(BidderProperties properties) {
        this.properties = properties;
        // Sensible pre-catalog default so ρ is finite before the first bid observes the size.
        this.totalBudget = properties.getCreativeBudget();
        String configured = properties.getCompetition().getStartTime();
        if (configured != null && !configured.isBlank()) {
            try {
                this.start = Instant.parse(configured.trim());
            } catch (Exception e) {
                log.warn("Unparseable competition.start-time '{}'; will start pacing clock on first bid", configured);
            }
        }
    }

    /** Bid multiplier for the current λ: {@code 1 / (1 + λ)}. Called on the bid hot path. */
    public double shadeFactor() {
        return 1.0 / (1.0 + lambda);
    }

    /** Current pacing multiplier (for metrics / debugging). */
    public double getLambda() {
        return lambda;
    }

    /**
     * Bid multiplier that scales up when actual spend lags the linear pace, so leftover budget gets
     * spent before the deadline instead of finishing the run under budget (the failure mode where we
     * win cheaply the whole way and end with money unspent).
     *
     * <p>Let {@code target = totalBudget · elapsed/duration} be the linear-pace spend for right now,
     * and {@code actual} our cumulative spend. The factor is {@code target / actual}, clipped to
     * {@code [1, catchUpMax]}: on or ahead of pace → 1.0 (no boost); behind → up to {@code catchUpMax}.
     * {@link #computeBidPrice} multiplies the anchor by this and {@code enforceConstraints} clamps the
     * result to the creative cap, so catch-up can raise bids toward the cap but never past it.
     *
     * <p>Returns 1.0 until the clock is running and we've spent something, so it never divides by
     * zero and never boosts before there's a pace to fall behind.
     */
    public double catchUpFactor() {
        Instant s = start;
        long durationSec = properties.getCompetition().getDurationSeconds();
        double actual = spentCents.get() / 100.0;
        if (s == null || durationSec <= 0 || actual <= 0) {
            return 1.0;
        }
        double elapsedSec = Duration.between(s, Instant.now()).toMillis() / 1000.0;
        // Fraction of the window elapsed, clamped to [0, 1] — past the deadline we still want the
        // full-budget target, not an ever-growing one.
        double fraction = Math.min(1.0, Math.max(0.0, elapsedSec / durationSec));
        double target = totalBudget * fraction;
        if (target <= actual) {
            return 1.0;
        }
        double factor = target / actual;
        return Math.min(factor, properties.getStrategy().getCatchUpMax());
    }

    /**
     * Count one auction we bid in, refresh the total budget from the live catalog size, and
     * lazily start the pacing clock if it wasn't configured.
     *
     * @param creativeCount number of creatives currently in our catalog (budget is per creative)
     */
    public void recordParticipation(int creativeCount) {
        participations.incrementAndGet();
        totalBudget = properties.getCreativeBudget() * Math.max(1, creativeCount);
        if (start == null) {
            start = Instant.now();
        }
    }

    /**
     * One dual-gradient step after observing an auction outcome.
     *
     * @param payment what we paid this auction: the clearing price if we won, 0.0 if we lost.
     */
    public void onOutcome(double payment) {
        // Accumulate what we actually spent (0 on a loss) so catchUpFactor can compare cumulative
        // spend against the linear-pace target. Rounded to cents to keep the running total exact.
        if (payment > 0) {
            spentCents.addAndGet(Math.round(payment * 100));
        }
        double rho = targetPerAuction();
        BidderProperties.Strategy s = properties.getStrategy();
        double next = lambda - s.getPacingEta() * (rho - payment);
        // Project back onto [0, λ_max]: λ is a nonnegative dual variable, and the cap keeps a
        // burst of expensive wins from pinning bids near zero for the rest of the run.
        next = Math.max(0.0, Math.min(next, s.getPacingLambdaMax()));
        lambda = next;
        log.debug("PACING payment={} rho={} lambda={} shade={}", payment, rho, next, shadeFactor());
    }

    /**
     * Estimated affordable spend per auction we participate in, {@code ρ ≈ B/T}.
     * B is the configured bidder budget; T is extrapolated from our participation rate across the
     * competition window. Falls back to {@code B / participations} before the clock is meaningful.
     */
    private double targetPerAuction() {
        long n = Math.max(1, participations.get());
        double budget = totalBudget;
        Instant s = start;
        long durationSec = properties.getCompetition().getDurationSeconds();
        if (s == null || durationSec <= 0) {
            return budget / n;
        }
        double elapsedSec = Math.max(1.0, Duration.between(s, Instant.now()).toMillis() / 1000.0);
        // Estimated total participations over the whole window = rate * duration.
        double estimatedTotal = (n / elapsedSec) * durationSec;
        return budget / Math.max(1.0, estimatedTotal);
    }
}
