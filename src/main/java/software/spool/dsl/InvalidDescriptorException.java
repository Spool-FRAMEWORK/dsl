package software.spool.dsl;

/**
 * A descriptor is missing a key or holds a value that cannot be used. The message starts with the
 * path of the offending key in the YAML, for example {@code modules[0].crawler.source is required}.
 *
 * <p>It extends {@link IllegalArgumentException} so callers that already catch that keep working.</p>
 */
public class InvalidDescriptorException extends IllegalArgumentException {
    private final String path;
    private final String problem;
    private final String context;

    public InvalidDescriptorException(String path, String problem) {
        this(path, problem, "");
    }

    private InvalidDescriptorException(String path, String problem, String context) {
        super(context + path + " " + problem);
        this.path = path;
        this.problem = problem;
        this.context = context;
    }

    /** Where in the descriptor the problem is, as a dotted path. */
    public String path() {
        return path;
    }

    /** The same problem, saying which module ran into it: {@code module 'x': path problem}. */
    public InvalidDescriptorException inModule(String module) {
        return new InvalidDescriptorException(path, problem, "module '" + module + "': ");
    }

    /**
     * The same problem, saying where the value comes from when the descriptor is made of several files or a
     * profile: {@code shared/modules.yaml: path problem}. An empty origin leaves the message as it is.
     */
    public InvalidDescriptorException from(String origin) {
        if (origin.isEmpty()) return this;
        return new InvalidDescriptorException(path, problem, origin + ": " + context);
    }
}
