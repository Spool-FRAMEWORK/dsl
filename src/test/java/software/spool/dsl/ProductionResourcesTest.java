package software.spool.dsl;

import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks what the published DSL jar carries, looking at the production classes rather than the test classpath. */
class ProductionResourcesTest {

    @Test
    void productionResources_haveNoDescriptor_soTheUsersOwnSpoolYamlIsTheOneLoaded() throws URISyntaxException {
        Path productionClasses = Path.of(SpoolNodeDSL.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        assertThat(productionClasses.resolve("Spool.yaml")).doesNotExist();
    }
}
