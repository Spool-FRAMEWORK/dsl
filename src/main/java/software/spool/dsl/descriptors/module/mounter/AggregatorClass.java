package software.spool.dsl.descriptors.module.mounter;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.mounter.api.port.MountAggregator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** The class a descriptor names to combine the records of a mount, checked when the descriptor is read. */
public record AggregatorClass(Constructor<? extends MountAggregator<?>> constructor) {

    public static AggregatorClass named(String path, String className) {
        Class<?> type = loaded(path, className);
        if (!MountAggregator.class.isAssignableFrom(type)) {
            throw new InvalidDescriptorException(path, className + " does not combine records: it is not a "
                    + MountAggregator.class.getSimpleName());
        }
        return new AggregatorClass(constructorWithoutArguments(path, type.asSubclass(aggregatorType())));
    }

    public MountAggregator<?> newAggregator() {
        try {
            return constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Could not create the aggregator " + constructor.getDeclaringClass().getName(), e);
        }
    }

    private static Class<?> loaded(String path, String className) {
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new InvalidDescriptorException(path, className + " is not on the class path");
        }
    }

    private static <T extends MountAggregator<?>> Constructor<T> constructorWithoutArguments(String path, Class<T> type) {
        try {
            return type.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new InvalidDescriptorException(path, type.getName() + " needs a public constructor without arguments");
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<MountAggregator<?>> aggregatorType() {
        return (Class) MountAggregator.class;
    }
}
