package software.spool.dsl.source;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Where descriptor files are read from, and how a file names another one next to it. */
public interface DescriptorSource {
    /** How the source reads in a message, for example {@code the classpath}. */
    String name();

    /** The content of the file at the location, or nothing when there is no such file. */
    Optional<byte[]> read(String location) throws IOException;

    /** The locations of every file in the folder and its subfolders, or nothing when there is no such folder. */
    Optional<List<String>> list(String folder) throws IOException;

    /** The location of {@code relative} taken from the folder that holds {@code from}. */
    String resolve(String from, String relative);
}
