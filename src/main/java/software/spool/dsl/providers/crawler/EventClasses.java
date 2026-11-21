package software.spool.dsl.providers.crawler;

import software.spool.core.model.Event;
import software.spool.dsl.InvalidDescriptorException;

import java.util.List;

/** Finds the event classes a crawler names by their simple name, in the package the descriptor says. */
final class EventClasses {
    static final String DEFAULT_PACKAGE = "events";

    private final String eventPackage;

    private EventClasses(String eventPackage) {
        this.eventPackage = eventPackage;
    }

    static EventClasses in(String eventPackage) {
        return new EventClasses(eventPackage);
    }

    List<Class<? extends Event>> resolve(String path, List<String> names) {
        return names.stream().<Class<? extends Event>>map(name -> resolve(path, name)).toList();
    }

    private Class<? extends Event> resolve(String path, String name) {
        Class<?> type = find(path, eventPackage + "." + name, name);
        if (!Event.class.isAssignableFrom(type)) {
            throw new InvalidDescriptorException(path, "names a class that is not an event: '" + type.getName() + "'");
        }
        return type.asSubclass(Event.class);
    }

    private Class<?> find(String path, String qualifiedName, String name) {
        try {
            return Class.forName(qualifiedName);
        } catch (ClassNotFoundException missing) {
            throw new InvalidDescriptorException(path,
                    "names a class that is not in package '" + eventPackage + "': '" + name + "'");
        }
    }
}
