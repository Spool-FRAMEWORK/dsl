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

    public InvalidDescriptorException(String path, String problem) {
        super(path + " " + problem);
        this.path = path;
        this.problem = problem;
    }

    /** Where in the descriptor the problem is, as a dotted path. */
    public String path() {
        return path;
    }

    /** The same problem, saying which module ran into it. */
    public InvalidDescriptorException neededBy(String module) {
        return new InvalidDescriptorException(path, problem + ", needed by " + module);
    }
}
