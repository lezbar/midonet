/*
 * Copyright 2012 Midokura Europe SARL
 */
package com.midokura.midonet.cluster.client;

import com.midokura.packets.IntIPv4;
import com.midokura.packets.MAC;
import com.midokura.util.functors.Callback1;


/**
 * Non-blocking.
 */
public interface ArpCache {
    void get(IntIPv4 ipAddr, Callback1<MAC> cb);
    void add(IntIPv4 ipAddr, MAC entry);
    void remove(IntIPv4 ipAddr);
}
