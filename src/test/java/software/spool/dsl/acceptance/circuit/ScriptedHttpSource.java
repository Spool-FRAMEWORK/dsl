package software.spool.dsl.acceptance.circuit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An HTTP source for the scenarios: it answers the responses it is given, in order, and then its usual one, and keeps
 * the time of every poll it receives.
 */
public final class ScriptedHttpSource implements AutoCloseable {
    private static final String PATH = "/orders";

    private final HttpServer server;
    private final Deque<Supplier<Response>> script = new ArrayDeque<>();
    private final List<Instant> polls = new ArrayList<>();
    private Response usual;

    public ScriptedHttpSource(String body) throws IOException {
        this.usual = new Response(200, Map.of(), body);
        this.server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext(PATH, this::answer);
        server.start();
    }

    public String url() {
        return "http://localhost:" + server.getAddress().getPort() + PATH;
    }

    /** A response made when the poll arrives, so a time it carries is counted from then. */
    public synchronized void answerOnce(Supplier<Response> response) {
        script.addLast(response);
    }

    public synchronized void answerEveryTime(Response response) {
        usual = response;
    }

    public synchronized List<Instant> polls() {
        return List.copyOf(polls);
    }

    public synchronized Optional<Instant> lastPoll() {
        return polls.isEmpty() ? Optional.empty() : Optional.of(polls.get(polls.size() - 1));
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void answer(HttpExchange exchange) throws IOException {
        Response response;
        synchronized (this) {
            polls.add(Instant.now());
            response = script.isEmpty() ? usual : script.removeFirst().get();
        }
        response.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(response.status(), body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
        exchange.close();
    }

    public record Response(int status, Map<String, String> headers, String body) {
        public static Response status(int status) {
            return new Response(status, Map.of(), "");
        }

        public static Response withHeader(int status, String name, String value) {
            return new Response(status, Map.of(name, value), "");
        }

        public static Response withBody(int status, String body) {
            return new Response(status, Map.of("Content-Type", "application/json"), body);
        }
    }
}
