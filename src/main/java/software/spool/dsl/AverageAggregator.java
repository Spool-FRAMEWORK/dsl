package software.spool.dsl;

import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.DoubleSummaryStatistics;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AverageAggregator implements MountAggregator<TickerPayload, AverageTickerPayload> {
    private record GroupingKey(String year, String source, String symbol) {}

    @Override
    public Stream<AverageTickerPayload> aggregate(Stream<PartitionedRecord<TickerPayload>> records) {
        return records
                .collect(Collectors.groupingBy(
                        partitioned -> {
                            String partitionPath = partitioned.partitionKey().value();
                            String year = extractAttribute(partitionPath, "year");
                            String month = extractAttribute(partitionPath, "month");
                            String symbol = partitioned.record().symbol();
                            return new GroupingKey(year, month, symbol);
                        },
                        Collectors.summarizingDouble(partitioned ->
                                Double.parseDouble(partitioned.record().bid()))
                ))
                .entrySet().stream()
                .map(entry -> {
                    GroupingKey key = entry.getKey();
                    DoubleSummaryStatistics stats = entry.getValue();

                    return new AverageTickerPayload(
                            key.year(),
                            key.source(),
                            key.symbol(),
                            stats.getAverage(),
                            (int) stats.getCount()
                    );
                });
    }

    private String extractAttribute(String path, String attributeName) {
        Pattern pattern = Pattern.compile(attributeName + "=([^/_-]+)");
        Matcher matcher = pattern.matcher(path);
        return matcher.find() ? matcher.group(1) : "unknown";
    }
}