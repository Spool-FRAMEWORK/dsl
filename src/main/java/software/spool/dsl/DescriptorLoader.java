package software.spool.dsl;

import software.spool.dsl.compose.ComposedDescriptor;
import software.spool.dsl.compose.DescriptorComposer;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.source.ClasspathDescriptorSource;
import software.spool.dsl.source.DescriptorSource;
import software.spool.dsl.source.FileSystemDescriptorSource;
import software.spool.dsl.yaml.DescriptorMapper;
import software.spool.dsl.yaml.YamlTreeParser;
import software.spool.dsl.variables.DescriptorVariables;
import software.spool.dsl.variables.EnvironmentValues;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Reads a descriptor with its includes and the profile chosen for it, and turns it into a
 * {@link SpoolNodeDescriptor}. Without a profile argument, the profile comes from {@value #PROFILE_VARIABLE},
 * and without that either, the descriptor is read as it is. Every error says the descriptor that was loaded
 * and, when it is not that file, the file and the profile the wrong value comes from.
 */
public final class DescriptorLoader {
    public static final String PROFILE_VARIABLE = "SPOOL_PROFILE";

    private final DescriptorComposer composer;
    private final UnaryOperator<String> environment;

    public DescriptorLoader(DescriptorSource source, UnaryOperator<String> environment) {
        this.composer = new DescriptorComposer(source, new YamlTreeParser());
        this.environment = environment;
    }

    public static DescriptorLoader fromClasspath() {
        return fromClasspath(System::getenv);
    }

    public static DescriptorLoader fromClasspath(UnaryOperator<String> environment) {
        return new DescriptorLoader(new ClasspathDescriptorSource(), environment);
    }

    public static DescriptorLoader fromFileSystem() {
        return fromFileSystem(System::getenv);
    }

    public static DescriptorLoader fromFileSystem(UnaryOperator<String> environment) {
        return new DescriptorLoader(new FileSystemDescriptorSource(), environment);
    }

    public SpoolNodeDescriptor load(String location) throws IOException {
        return load(location, profileOfTheEnvironment(), Function.identity());
    }

    public SpoolNodeDescriptor load(String location, String profile) throws IOException {
        return load(location, Optional.of(profile), Function.identity());
    }

    /**
     * Loads the descriptor and hands it to {@code build}, so that what goes wrong while building is reported
     * with the file and the profile the wrong value comes from, like any other error of the descriptor.
     */
    public <T> T load(String location, Optional<String> profile, Function<SpoolNodeDescriptor, T> build)
            throws IOException {
        ComposedDescriptor composed;
        try {
            composed = composer.compose(location, profile);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (InvalidDescriptorException e) {
            throw new IOException("Invalid descriptor " + location + ": " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IOException("Could not load descriptor " + location + ": " + e.getMessage(), e);
        }
        EnvironmentValues environmentValues = new EnvironmentValues();
        try {
            Map<String, Object> tree = new DescriptorVariables(environment, environmentValues).resolve(composed.tree());
            return build.apply(DescriptorMapper.map(RawSpoolNodeDescriptor.from(tree)));
        } catch (InvalidDescriptorException e) {
            InvalidDescriptorException located =
                    e.from(composed.origins().originOf(e.path()).describe(location));
            throw failure("Invalid descriptor " + location + ": " + located.getMessage(), located, environmentValues);
        } catch (Exception e) {
            throw failure("Could not load descriptor " + location + ": " + e.getMessage(), e, environmentValues);
        }
    }

    /**
     * The error to throw, with no value read from the environment in it. When the message held one, the cause is
     * left out, because its own message and stack trace would show the value again.
     */
    private static IOException failure(String message, Exception cause, EnvironmentValues environmentValues) {
        String masked = environmentValues.mask(message);
        return masked.equals(message) ? new IOException(message, cause) : new IOException(masked);
    }

    public Optional<String> profileOfTheEnvironment() {
        return Optional.ofNullable(environment.apply(PROFILE_VARIABLE)).filter(name -> !name.isBlank());
    }
}
