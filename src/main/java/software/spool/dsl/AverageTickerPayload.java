package software.spool.dsl;

public record AverageTickerPayload(String year, String month, String symbol, Double averageBid, int samples) {}

