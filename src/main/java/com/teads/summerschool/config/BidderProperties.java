package com.teads.summerschool.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bidder")
public class BidderProperties {

    private String id = "teads-bidder";
    // Flat budget assigned to each creative on seed; remaining is tracked per creative in Redis.
    private double creativeBudget = 25.0;
    private long timeoutMs = 1000;
    private Strategy strategy = new Strategy();
    private Competition competition = new Competition();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }


    public double getCreativeBudget() { return creativeBudget; }
    public void setCreativeBudget(double creativeBudget) { this.creativeBudget = creativeBudget; }

    public long getTimeoutMs() { return timeoutMs; }
    public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }

    public Strategy getStrategy() { return strategy; }
    public void setStrategy(Strategy strategy) { this.strategy = strategy; }

    public Competition getCompetition() { return competition; }
    public void setCompetition(Competition competition) { this.competition = competition; }

    public static class Strategy {
        private int minSamples = 10;
        private double coldStartMultiplier = 1.15;
        private int windowSize = 50;
        private double marketMultiplier = 1.05;
        private double premiumMultiplier = 1.5;
        private double pacingBoost = 1.20;
        private double pacingCut = 0.85;
        // Adaptive-pacing (dual gradient) knobs — see PacingController.
        // eta: step size of the λ update; lambdaMax: cap on the pacing multiplier.
        private double pacingEta = 0.5;
        private double pacingLambdaMax = 4.0;
        // A loss clearing price above (current rolling average × this) is treated as an outlier
        // (e.g. a competitor's fat-finger overbid) and clamped to that ceiling before it enters
        // the market window — so one crazy-high winning bid can't drag our anchor up and make us
        // overpay. See BidderStatsCache.recordMarketPrice.
        private double marketOutlierMultiplier = 3.0;
        // Late-run budget catch-up (see PacingController.catchUpFactor). Only engages in the final
        // stretch of the competition — being under linear pace earlier is normal and correct in a
        // cheap first-price market, so boosting then just overpays and wins fewer impressions per
        // dollar (the bug in the original always-on 3× version). catchUpStartFraction: fraction of
        // the window that must elapse before catch-up turns on (0.75 = final 25%). catchUpMax: the
        // gentle ceiling on the boost — deliberately small so we spend leftover budget without
        // grossly overpaying. enforceConstraints still clamps the result to the creative cap.
        private double catchUpStartFraction = 0.75;
        private double catchUpMax = 1.25;

        public int getMinSamples() { return minSamples; }
        public void setMinSamples(int minSamples) { this.minSamples = minSamples; }

        public double getColdStartMultiplier() { return coldStartMultiplier; }
        public void setColdStartMultiplier(double coldStartMultiplier) { this.coldStartMultiplier = coldStartMultiplier; }

        public int getWindowSize() { return windowSize; }
        public void setWindowSize(int windowSize) { this.windowSize = windowSize; }

        public double getMarketMultiplier() { return marketMultiplier; }
        public void setMarketMultiplier(double marketMultiplier) { this.marketMultiplier = marketMultiplier; }

        public double getPremiumMultiplier() { return premiumMultiplier; }
        public void setPremiumMultiplier(double premiumMultiplier) { this.premiumMultiplier = premiumMultiplier; }

        public double getPacingBoost() { return pacingBoost; }
        public void setPacingBoost(double pacingBoost) { this.pacingBoost = pacingBoost; }

        public double getPacingCut() { return pacingCut; }
        public void setPacingCut(double pacingCut) { this.pacingCut = pacingCut; }

        public double getPacingEta() { return pacingEta; }
        public void setPacingEta(double pacingEta) { this.pacingEta = pacingEta; }

        public double getPacingLambdaMax() { return pacingLambdaMax; }
        public void setPacingLambdaMax(double pacingLambdaMax) { this.pacingLambdaMax = pacingLambdaMax; }

        public double getMarketOutlierMultiplier() { return marketOutlierMultiplier; }
        public void setMarketOutlierMultiplier(double marketOutlierMultiplier) { this.marketOutlierMultiplier = marketOutlierMultiplier; }

        public double getCatchUpStartFraction() { return catchUpStartFraction; }
        public void setCatchUpStartFraction(double catchUpStartFraction) { this.catchUpStartFraction = catchUpStartFraction; }

        public double getCatchUpMax() { return catchUpMax; }
        public void setCatchUpMax(double catchUpMax) { this.catchUpMax = catchUpMax; }
    }

    public static class Competition {
        // ISO-8601 instant, e.g. "2026-06-01T09:00:00Z". Empty = pacing disabled.
        private String startTime = "";
        private long durationSeconds = 1800;

        public String getStartTime() { return startTime; }
        public void setStartTime(String startTime) { this.startTime = startTime; }

        public long getDurationSeconds() { return durationSeconds; }
        public void setDurationSeconds(long durationSeconds) { this.durationSeconds = durationSeconds; }
    }
}
