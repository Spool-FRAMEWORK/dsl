package software.spool.dsl.acceptance.pieces;

import io.cucumber.java.en.Then;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.descriptors.DeclaredModule;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class InfrastructurePieceSteps {
    private final DescriptorUnderTest descriptor;

    public InfrastructurePieceSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Then("the module {string} uses the piece {string} as its inbox")
    public void theModuleUsesThePieceAsItsInbox(String id, String piece) {
        SpoolNodeDescriptor loaded = descriptor.loaded();
        DeclaredModule module = loaded.declaredModules().stream()
                .filter(declared -> declared.descriptor().id().equals(id))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the descriptor has no module " + id));
        assertThat(loaded.infrastructureOf(module).inbox().name()).isEqualTo(piece);
    }

    @Then("the node sends its heartbeats to {string}")
    public void theNodeSendsItsHeartbeatsTo(String url) {
        assertThat(descriptor.loaded().watchdog()).contains(url);
    }

    @Then("the piece {string} has the path {string}")
    public void thePieceHasThePath(String name, String path) {
        assertThat(descriptor.loaded().infrastructure().piece(name))
                .hasValueSatisfying(piece -> assertThat(piece.configuration()).containsEntry("path", path));
    }
}
