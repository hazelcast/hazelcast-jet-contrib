package com.hazelcast.jet.contrib.http;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * TODO: Javadoc
 */
public final class HttpServerSource {

    private HttpServerSource() {
    }

    /**
     * @param portOffset
     * @return
     */
    @Nonnull
    public static StreamSource<JsonValue> httpServer(@Nonnull int portOffset) {
        checkPositive(portOffset, "portOffset cannot be negative");

        return SourceBuilder.stream("http-listener(base port +" + portOffset + ")", context -> new HttpServerSourceContext(context.jetInstance(), portOffset))
                            .fillBufferFn(HttpServerSourceContext::fillBuffer)
                            .destroyFn(HttpServerSourceContext::close)
                            .distributed(1)
                            .build();
    }

    /**
     * @param portOffset
     * @param sslContextSupplier
     * @return
     */
    @Nonnull
    public static StreamSource<JsonValue> httpsServer(@Nonnull int portOffset, @Nonnull Supplier<SSLContext> sslContextSupplier) {
        checkPositive(portOffset, "portOffset cannot be negative");
        checkNotNull(sslContextSupplier, "sslContextSupplier cannot be null");

        return SourceBuilder.stream("https-listener(base port + " + portOffset + ")", context -> new HttpServerSourceContext(context.jetInstance(), portOffset, sslContextSupplier.get()))
                            .fillBufferFn(HttpServerSourceContext::fillBuffer)
                            .destroyFn(HttpServerSourceContext::close)
                            .distributed(1)
                            .build();
    }

    private static class HttpServerSourceContext {

        private static final ILogger LOGGER = Logger.getLogger(HttpServerSource.class);
        private static final int MAX_FILL_ELEMENTS = 100;

        private final BlockingQueue<JsonValue> queue = new ArrayBlockingQueue<>(1000);
        private final ArrayList<JsonValue> buffer = new ArrayList<>(MAX_FILL_ELEMENTS);
        private final Undertow undertow;
        private final HttpHandler handler = exchange ->
                exchange
                        .getRequestReceiver()
                        .receiveFullString((e, message) -> {
                            queue.offer(Json.parse(message));
                            e.endExchange();
                        });

        HttpServerSourceContext(JetInstance jet, int portOffset) {
            this(jet, portOffset, null);
        }

        HttpServerSourceContext(JetInstance jet, int portOffset, @Nullable SSLContext sslContext) {
            Address localAddress = jet.getHazelcastInstance().getCluster().getLocalMember().getAddress();
            String host = localAddress.getHost();
            int port = localAddress.getPort() + portOffset;
            if (sslContext != null) {
                undertow = Undertow.builder()
                                   .addHttpsListener(port, host, sslContext)
                                   .setHandler(handler)
                                   .build();
                LOGGER.info("Starting to listen HTTPS messages on https://" + host + ":" + port);
            } else {
                undertow = Undertow.builder()
                                   .addHttpListener(port, host)
                                   .setHandler(handler)
                                   .build();
                LOGGER.info("Starting to listen HTTP messages on http://" + host + ":" + port);
            }
            undertow.start();
        }

        void fillBuffer(SourceBuffer<JsonValue> sourceBuffer) {
            queue.drainTo(buffer, MAX_FILL_ELEMENTS);
            for (JsonValue json : buffer) {
                sourceBuffer.add(json);
            }
            buffer.clear();
        }

        void close() {
            undertow.stop();
        }
    }
}
