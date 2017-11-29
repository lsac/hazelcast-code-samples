package com.hazelcast.interview;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * A simple test of a map
 * Having the caller to print stats based on time elapsed since last print
 */
public final class SimpleMapPutTest1 {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 5;

    private final HazelcastInstance instance;
    private final ILogger logger;
    private final Stats stats = new Stats();
    private final Random random;

    private final int threadCount;
    private final int entryCount;
    private final int valueSize;

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    private SimpleMapPutTest1(int threadCount, int entryCount, int valueSize) {
        this.threadCount = threadCount;
        this.entryCount = entryCount;
        this.valueSize = valueSize;
        Config cfg = new XmlConfigBuilder().build();

        instance = Hazelcast.newHazelcastInstance(cfg);
        logger = instance.getLoggingService().getLogger("SimpleMapPutTest");
        random = new Random();
    }

    /**
     * Expects the Management Center to be running.
     */
    public static void main(String[] input) throws InterruptedException {
        int threadCount = 40;
        int entryCount = 10 * 1000;
        int valueSize = 1000;

        SimpleMapPutTest1 test = new SimpleMapPutTest1(threadCount, entryCount, valueSize);
        test.start();
    }

    private void start() throws InterruptedException {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        run(es);
    }

    private void run(ExecutorService es) {
        final IMap<String, Object> map = instance.getMap(NAMESPACE);
        for (int i = 0; i < threadCount; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        while (true) {
                            int key = (int) (random.nextFloat() * entryCount);
                            map.put(String.valueOf(key), createValue());
                            stats.doIncrement();
                        }
                    } catch (HazelcastInstanceNotActiveException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private Object createValue() {
        return new byte[valueSize];
    }

    /**
     * A basic statistics class
     */
    private class Stats {

        private AtomicLong puts = new AtomicLong();
        private AtomicLong lastPutTime = new AtomicLong();
        private Lock lock;

        long doIncrement() {
            lock = instance.getLock("COUNTER");
            lock.lock();
            try {
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastPutTime.get() >= (STATS_SECONDS * 1000)) {
                    lastPutTime.set(currentTime);
                    printAndReset();
                }
                return stats.puts.incrementAndGet();
            } finally {
                lock.unlock();
            }
        }

        void printAndReset() {
            long putsNow = puts.getAndSet(0);

            logger.info("puts: " + putsNow);
            logger.info("Operations per Second: " + putsNow / STATS_SECONDS);
        }
    }

    private void printVariables() {
        logger.info("Starting Test with ");
        logger.info("Thread Count: " + threadCount);
        logger.info("Sampling interval: " + STATS_SECONDS);
    }
}
