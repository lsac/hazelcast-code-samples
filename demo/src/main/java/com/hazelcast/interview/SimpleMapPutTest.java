/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * A simple test of a map.
 */
public final class SimpleMapPutTest {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

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

    private SimpleMapPutTest(int threadCount, int entryCount, int valueSize) {
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

        SimpleMapPutTest test = new SimpleMapPutTest(threadCount, entryCount, valueSize);
        test.start();
    }

    private void start() throws InterruptedException {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        startPrintStats();
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
                            // more lock means more latency, no need for a lock here for updating the stats
                            map.put(String.valueOf(key), createValue());
                            stats.puts.incrementAndGet();
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

    private void startPrintStats() {
        Thread thread = new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        stats.printAndReset();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        };
        thread.start();
    }

    /**
     * A basic statistics class
     */
    private class Stats {

        private AtomicLong puts = new AtomicLong();

        void printAndReset() {
            long putsNow = puts.getAndSet(0);

            logger.info("puts: " + putsNow);
            logger.info("Operations per Second: " + putsNow / STATS_SECONDS);
        }
    }

    private void printVariables() {
        logger.info("Starting Test with ");
        logger.info("Thread Count: " + threadCount);
    }
}
