package software.spool.dsl;

import java.util.List;

public record TickerPayload(
        String symbol,
        String open,
        String high,
        String low,
        String close,
        List<String> changes,
        String bid,
        String ask
) {}