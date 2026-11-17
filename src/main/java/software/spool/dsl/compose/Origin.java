package software.spool.dsl.compose;

import java.util.Optional;

/** The file a value comes from and, when a profile set it, that profile. */
public record Origin(String location, Optional<String> profile) {

    public static Origin of(String location) {
        return new Origin(location, Optional.empty());
    }

    public Origin inProfile(String name) {
        return new Origin(location, Optional.of(name));
    }

    /**
     * How the origin reads in front of an error of the descriptor loaded from {@code root}: empty for the
     * root itself, the file for an included one, and the profile in brackets when a profile set the value.
     */
    public String describe(String root) {
        Optional<String> file = location.equals(root) ? Optional.empty() : Optional.of(location);
        Optional<String> setBy = profile.map(name -> "profile " + name);
        if (file.isPresent() && setBy.isPresent()) return file.get() + " (" + setBy.get() + ")";
        return file.or(() -> setBy).orElse("");
    }
}
