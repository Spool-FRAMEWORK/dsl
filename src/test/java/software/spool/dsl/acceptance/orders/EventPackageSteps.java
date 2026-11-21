package software.spool.dsl.acceptance.orders;

import io.cucumber.java.en.Then;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

public class EventPackageSteps {
    private final DescriptorUnderTest descriptor;

    public EventPackageSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Then("the crawler publishes the event class {string}")
    public void theCrawlerPublishesTheEventClass(String name) {
        CrawlerDescriptor crawler = (CrawlerDescriptor) descriptor.loaded().modules().getFirst();
        assertThat(crawler.eventMapping().domainEvents()).extracting(Class::getName).containsExactly(name);
    }
}
