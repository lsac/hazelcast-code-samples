package com.hazelcast.interview;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Log3 implements IConstant {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        NetworkConfig network = config.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().addMember("192.168.4.12").addMember("192.168.4.159").setEnabled(true);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        hz.getList("any");
        int size = hz.getCluster().getMembers().size();
        if (size > NODE_LIMIT) {
            System.out.printf("Only %d nodes are allowed, no more!\n", NODE_LIMIT);
            hz.shutdown();
        }
        if (size == 1) {
            System.out.println("We are started");
        } else {
            System.out.println("Log was printed");
        }
    }

}
