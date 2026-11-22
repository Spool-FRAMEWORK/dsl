package software.spool.dsl.providers.mounter;

import software.spool.core.model.spool.SpoolModule;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.polling.ThreadedPollingScheduler;
import software.spool.core.port.quarantine.QuarantinedResultWriter;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.ScheduleDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.mounter.AggregatorClass;
import software.spool.dsl.descriptors.module.mounter.MounterDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterType;
import software.spool.dsl.descriptors.module.mounter.PartitioningDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.AttributesPartitioning;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.adapter.PartitioningQuarantinedResultWriter;
import software.spool.mounter.api.adapter.PassThroughMountAggregator;
import software.spool.mounter.api.adapter.SameAsSourcePartitioning;
import software.spool.mounter.api.builder.CheckpointFacet;
import software.spool.mounter.api.builder.MountFacet;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.builder.PollingMounterBuilder;
import software.spool.mounter.api.builder.QuarantineFacet;
import software.spool.mounter.api.builder.ReactiveMounterBuilder;
import software.spool.mounter.api.builder.ValidationFacet;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.MountPartitioning;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedReader;

/**
 * Builds the mounter of a descriptor. A {@code TRANSFORMATION} mount reads bronze and writes silver in the data lake,
 * with the quarantine of the infrastructure; an {@code AGGREGATION} mount reads silver and writes gold in the data
 * mart. What the descriptor does not say keeps the plainest choice: records are copied as they are, every partition
 * is ready to mount and nothing is remembered between rounds.
 */
@SpoolPlugin(SpoolModuleProvider.class)
public class MounterSpoolModuleProvider implements SpoolModuleProvider {

    @Override
    public String name() {
        return "MOUNTER";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.get("descriptor", SpoolModuleDescriptor.class)
                .filter(d -> d instanceof MounterDescriptor)
                .isPresent();
    }

    @Override
    public SpoolModule create(PluginConfiguration configuration) {
        MounterDescriptor mounter = configuration.require("descriptor", MounterDescriptor.class);
        InfrastructureDescriptor infrastructure = configuration.require("infrastructure", InfrastructureDescriptor.class);
        PartitionedReader reader = InfrastructurePluginFactory.partitionedReader(infrastructure);
        return mounter.type() == MounterType.POLL
                ? polling(mounter, infrastructure, reader)
                : reactive(mounter, infrastructure, reader);
    }

    private static SpoolModule polling(MounterDescriptor mounter, InfrastructureDescriptor infrastructure, PartitionedReader reader) {
        PollingMounterBuilder<Object, Object> builder = MounterBuilderFactory
                .watchdog(infrastructure.watchdog(), mounter.id())
                .<Object, Object>polling(reader)
                .aggregatingWith(aggregator(mounter));
        configure(mounter, infrastructure, builder.mount(), builder.checkpoint(), builder.quarantine(), builder.validation());
        builder.scheduling()
                .pollingWith(PollingPolicy.every(mounter.schedule().map(ScheduleDescriptor::every).orElseThrow()))
                .scheduledWith(new ThreadedPollingScheduler())
                .emittingWith(InfrastructurePluginFactory.tracedEventPublisher(infrastructure));
        return builder.build();
    }

    private static SpoolModule reactive(MounterDescriptor mounter, InfrastructureDescriptor infrastructure, PartitionedReader reader) {
        ReactiveMounterBuilder<Object, Object> builder = MounterBuilderFactory
                .watchdog(infrastructure.watchdog(), mounter.id())
                .<Object, Object>reactive(InfrastructurePluginFactory.eventBus(infrastructure))
                .readingWith(reader)
                .aggregatingWith(aggregator(mounter));
        configure(mounter, infrastructure, builder.mount(), builder.checkpoint(), builder.quarantine(), builder.validation());
        return builder.build();
    }

    private static void configure(MounterDescriptor mounter, InfrastructureDescriptor infrastructure, MountFacet<?> mount,
                                  CheckpointFacet<?> checkpoint, QuarantineFacet<?> quarantine, ValidationFacet<?> validation) {
        mount.onTarget(targetOf(mounter))
                .writingWith(writer(mounter, infrastructure))
                .partitioningWith(partitioning(mounter.write().partitioning()));
        checkpoint.windowPolicy(new AlwaysClosedWindowPolicy()).checkpoint(new NoOpMountCheckpoint());
        if (mounter.mode() == MountMode.TRANSFORMATION) {
            quarantine.in(InfrastructurePluginFactory.quarantineRepository(infrastructure))
                    .removingFromDataLakeWith(InfrastructurePluginFactory.dataLakeEnvelopeRemover(infrastructure));
        } else if (infrastructure.find(InfrastructureKind.QUARANTINE_REPOSITORY).isPresent()) {
            quarantine.in(InfrastructurePluginFactory.quarantineRepository(infrastructure));
        }
        if (mounter.validate()) {
            validation.enabled();
        }
    }

    /**
     * Writes the quarantined results of a mounter back to its data mart, with the writer and the partitioning the
     * mounter writes with.
     */
    public static QuarantinedResultWriter quarantinedResultWriter(MounterDescriptor mounter, InfrastructureDescriptor infrastructure) {
        return new PartitioningQuarantinedResultWriter(targetOf(mounter), partitioning(mounter.write().partitioning()),
                writer(mounter, infrastructure));
    }

    private static MountTarget targetOf(MounterDescriptor mounter) {
        return new MountTarget(mounter.write().dataMart(), mounter.read().partition(), mounter.mode(), null, null);
    }

    private static DataMartWriter writer(MounterDescriptor mounter, InfrastructureDescriptor infrastructure) {
        return mounter.mode() == MountMode.TRANSFORMATION
                ? InfrastructurePluginFactory.dataLakeMountWriter(infrastructure)
                : InfrastructurePluginFactory.dataMartWriter(infrastructure);
    }

    private static MountPartitioning<Object> partitioning(PartitioningDescriptor partitioning) {
        return switch (partitioning.type()) {
            case SAME_AS_SOURCE -> new SameAsSourcePartitioning<>();
            case ATTRIBUTES -> AttributesPartitioning.of(partitioning.attributes());
        };
    }

    @SuppressWarnings("unchecked")
    private static MountAggregator<Object> aggregator(MounterDescriptor mounter) {
        return (MountAggregator<Object>) mounter.aggregator()
                .map(AggregatorClass::newAggregator)
                .orElseGet(PassThroughMountAggregator::new);
    }
}
