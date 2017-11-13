package com.hazelcast.interview;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import java.util.List;
/**
 * This sample uses non-partitioned list size to do logging.
 * By checking the list size, the code is able to 
 * determine if the current node is the first running node in the 
 * list to log the message
 * 
 * @author wsang
 */
public class Log2 implements IConstant {
    public static void main(String[] args) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        final List<String> list = hz.getList("Log2");
        
        if (list.size() >= NODE_LIMIT) {
            System.out.printf("Only %d nodes are allowed, no more!\n", NODE_LIMIT);
            hz.shutdown();
        } else {
            list.add("LOG2");
            System.out.println(list.size() == 1 ? "We are started!" : "Log was printed!");
        }
    }

}
