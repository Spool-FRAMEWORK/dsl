package software.spool.dsl;

import software.spool.crawler.api.port.source.PollSource;
import software.spool.crawler.internal.adapter.http.HTTPPollSource;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

public class SourceFactory {
    public static PollSource<?> pollFrom(SourceDescriptor descriptor) {
        return switch (descriptor.type()) {
            case POLL -> getFrom(descriptor);
            case STREAM -> null;
            case WEBHOOK -> null;
        };
    }

    private static PollSource<?> getFrom(SourceDescriptor descriptor) {
        return switch (descriptor.poll().type()) {
            case HTTP -> new HTTPPollSource(descriptor.poll().http().url(), descriptor.id());
            case DATABASE -> null;
            case FILE -> null;
            case CUSTOM -> null;
        };
    }

    public static PollSource<?> http(String url, String sourceId) {
        return new HTTPPollSource(url, sourceId);
    }
}
