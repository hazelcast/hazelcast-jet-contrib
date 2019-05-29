package com.hazelcast.jet.contrib.localcollector;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

public class LocalCollectorTest {
    private static final AtomicLong GLOBAL_COUNTER = new AtomicLong();

    private JetInstance server1;
    private JetInstance server2;

    private JetInstance client1;
    private JetInstance client2;

    @Before
    public void setUp() {
        GLOBAL_COUNTER.set(0);

        server1 = Jet.newJetInstance();
        server2 = Jet.newJetInstance();

        client1 = Jet.newJetClient();
        client2 = Jet.newJetClient();
    }

    @After
    public void tearDown() {
        if (client2 != null) {
            client2.shutdown();
        }
        if (client1 != null) {
            client1.shutdown();
        }
        if (server2 != null) {
            server2.shutdown();
        }
        if (server1 != null) {
            server1.shutdown();
        }
    }

    @Test
    public void testLocalCollector() {
        long sourceEmittingNanos = TimeUnit.SECONDS.toNanos(15);

        AtomicLong counter = new AtomicLong();
        LocalCollector<Long> collector = LocalCollector.<Long>createNew(client1)
                .consumer(counter::addAndGet)
                .exceptionConsumer(Throwable::printStackTrace)
                .start();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(timeboundSource(sourceEmittingNanos))
                .withIngestionTimestamps()
                .window(tumbling(1000))
                .aggregate(counting())
                .map(WindowResult::result)
                .drainTo(collector.asSink());

        client1.newJob(pipeline).join();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(GLOBAL_COUNTER.get(), counter.get())
        );
    }

    @Test
    public void testLocalCollector_withReconnect_withExplicitOffset() throws InterruptedException {
        long sourceEmittingNanos = TimeUnit.SECONDS.toNanos(20);
        String collectorName = "myCollector";

        ReconnectableSummingConsumer consumer1 = new ReconnectableSummingConsumer();
        LocalCollector<Long> collector1 = LocalCollector.<Long>createNew(client1)
                .consumer(consumer1::accept)
                .exceptionConsumer(System.out::println)
                .name(collectorName)
                .start();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(timeboundSource(sourceEmittingNanos))
                .withIngestionTimestamps()
                .window(tumbling(1000))
                .aggregate(counting())
                .map(WindowResult::result)
                .drainTo(collector1.asSink());

        client1.newJob(pipeline);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        client1.shutdown();

        ReconnectableSummingConsumer consumer2 = new ReconnectableSummingConsumer();
        LocalCollector<Long> collector2 = LocalCollector.<Long>reconnect(client2)
                .fromOffset(consumer1.highestOffset + 1)
                .consumer(consumer2::accept)
                .name(collectorName)
                .start();


        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
                    long c1 = consumer1.sum;
                    long c2 = consumer2.sum;
                    assertEquals(GLOBAL_COUNTER.get(), c1 + c2);
                    System.out.println("Yes!!");
                }
        );
    }

    @Test
    public void testLocalCollector_withReconnect_withDefaultOffset() throws InterruptedException {
        long sourceEmittingNanos = TimeUnit.SECONDS.toNanos(20);
        String collectorName = "myCollector";

        AtomicLong counter = new AtomicLong();
        ReconnectableSummingConsumer consumer1 = new ReconnectableSummingConsumer();
        LocalCollector<Long> collector1 = LocalCollector.<Long>createNew(client1)
                .consumer(consumer1::accept)
                .exceptionConsumer(System.out::println)
                .name(collectorName)
                .start();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(timeboundSource(sourceEmittingNanos))
                .withIngestionTimestamps()
                .window(tumbling(1000))
                .aggregate(counting())
                .map(WindowResult::result)
                .drainTo(collector1.asSink());

        client1.newJob(pipeline);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        client1.shutdown();

        ReconnectableSummingConsumer consumer2 = new ReconnectableSummingConsumer();
        LocalCollector<Long> collector2 = LocalCollector.<Long>reconnect(client2)
                .fromLatest()
                .consumer(consumer2::accept)
                .name(collectorName)
                .start();


        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
                    long c1 = consumer1.sum;
                    long c2 = consumer2.sum;
                    assertTrue(c1 + c2 <= GLOBAL_COUNTER.get());
                }
        );
    }

    private static class ReconnectableSummingConsumer {
        private volatile long highestOffset;
        private volatile long sum;

        public void accept(long offset, long item) {
            highestOffset = Math.max(highestOffset, offset);
            sum += item;
        }
    }


    private static StreamSource<Long> timeboundSource(long durationNanos) {
        return SourceBuilder.stream("timebound-source", c -> new AtomicLong(System.nanoTime() + durationNanos))
                .<Long>fillBufferFn((c, b) -> {
                    long now = System.nanoTime();
                    if (now >= c.get()) {
                        b.close();
                        return;
                    }
                    b.add(System.nanoTime());
                    GLOBAL_COUNTER.incrementAndGet();
                })
                .distributed(1)
                .build();
    }

}