package com.hazelcast.interview;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IFunction;
/**
 * This sample uses IAtomicLong to do logging.
 * By checking the atomic long value, the code is able to 
 * determine if the current node is the first running node to log 
 * the message
 * 
 * @author wsang
 */
public class Log1 implements IConstant {
    public static void main(String[] args) {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        IAtomicLong atomicLong = hazelcastInstance.getAtomicLong("counter");

        long result = atomicLong.getAndAlter(new IFunction<Long, Long>() {
            @Override
            public Long apply(Long input) {
                return input + 1;
            }
        });
        System.out.printf("getAndAlter.result = %d, value = %d\n", result, atomicLong.get());
        if (result == 0) {
            System.out.println("We are started!");
        } else if (result < NODE_LIMIT) {
            System.out.println("Log was printed!");
        } else {
            System.out.printf("Only %d nodes are allowed, no more!\n", NODE_LIMIT);
            hazelcastInstance.shutdown();
        }
    }
}
