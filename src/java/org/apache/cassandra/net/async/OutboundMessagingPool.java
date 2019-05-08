/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Groups a set of outbound connections to a given peer, and routes outgoing messages to the appropriate connection
 * (based upon message's type or size). Contains a {@link OutboundMessagingConnection} for each of the
 * {@link ConnectionType} type.
 */
public class OutboundMessagingPool
{
    @VisibleForTesting
    static final long LARGE_MESSAGE_THRESHOLD = Long.getLong(Config.PROPERTY_PREFIX + "otcp_large_message_threshold", 1024 * 64);

    private final ConnectionMetrics metrics;
    private final BackPressureState backPressureState;
    //Optional strategy

    private final int coalescingWindowMs = DatabaseDescriptor.getOtcCoalescingWindow()*90/(100*1000);
    private final int numOptionalOutboundChannels = DatabaseDescriptor.getNumOptionalOutboundChannels();
    private final String optionalOutboundChannelsStrategy = DatabaseDescriptor.getOptionalOutboundChannelsStrategy();
    private final AtomicInteger nextIndexToUse;
    private Long lastIndexChangeInNs;
    private final boolean optionalDisabled;



    public OutboundMessagingConnection gossipChannel;
    public OutboundMessagingConnection largeMessageChannel;
    public OutboundMessagingConnection smallMessageChannel;
    public List<OutboundMessagingConnection> optionalChannels;

    /**
     * An override address on which to communicate with the peer. Typically used for something like EC2 public IP addresses
     * which need to be used for communication between EC2 regions.
     */
    private InetAddressAndPort preferredRemoteAddr;

    public OutboundMessagingPool(InetAddressAndPort remoteAddr, InetAddressAndPort localAddr, ServerEncryptionOptions encryptionOptions,
                                 BackPressureState backPressureState, IInternodeAuthenticator authenticator)
    {
        preferredRemoteAddr = remoteAddr;
        this.backPressureState = backPressureState;
        metrics = new ConnectionMetrics(localAddr, this);


        smallMessageChannel = new OutboundMessagingConnection(OutboundConnectionIdentifier.small(localAddr, preferredRemoteAddr),
                                                              encryptionOptions, coalescingStrategy(remoteAddr), authenticator);
        largeMessageChannel = new OutboundMessagingConnection(OutboundConnectionIdentifier.large(localAddr, preferredRemoteAddr),
                                                              encryptionOptions, coalescingStrategy(remoteAddr), authenticator);

        // don't attempt coalesce the gossip messages, just ship them out asap (let's not anger the FD on any peer node by any artificial delays)
        gossipChannel = new OutboundMessagingConnection(OutboundConnectionIdentifier.gossip(localAddr, preferredRemoteAddr),
                                                        encryptionOptions, Optional.empty(), authenticator);

        //Optional strategy
        if(!optionalOutboundChannelsStrategy.equalsIgnoreCase("DISABLED"))
        {
            optionalDisabled=false;
            nextIndexToUse = new AtomicInteger(0);
            lastIndexChangeInNs = System.nanoTime();
            optionalChannels = new ArrayList<>();
            for (int index = 0; index < numOptionalOutboundChannels; index++)
            {
                optionalChannels.add(
                new OutboundMessagingConnection(OutboundConnectionIdentifier.optional(index, localAddr, preferredRemoteAddr),
                                                encryptionOptions, coalescingStrategy(remoteAddr), authenticator)
                );
            }
        }
        else{
            optionalDisabled=true;
            nextIndexToUse=null;
            lastIndexChangeInNs=null;
        }
    }

    private static Optional<CoalescingStrategy> coalescingStrategy(InetAddressAndPort remoteAddr)
    {
        String strategyName = DatabaseDescriptor.getOtcCoalescingStrategy();
        String displayName = remoteAddr.toString();
        return CoalescingStrategies.newCoalescingStrategy(strategyName,
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
                                                          OutboundMessagingConnection.logger,
                                                          displayName);

    }

    public BackPressureState getBackPressureState()
    {
        return backPressureState;
    }

    public void sendMessage(MessageOut msg, int id)
    {
        getConnection(msg).sendMessage(msg, id);
    }

    private OutboundMessagingConnection getOutboundSmallChannel(int index){
        if(index==0) return smallMessageChannel;
        index--;
        return optionalChannels.get(index);
    }

    private int getModuloIndex(int counter){
        return counter%(numOptionalOutboundChannels+1);
    }

    private OutboundMessagingConnection fixedWindowStrategy(){
        long currentNs = System.nanoTime();
        int moduloIdx=-1;
        synchronized(lastIndexChangeInNs){
            long diffMs = (currentNs - lastIndexChangeInNs)/(1000000);
            if(diffMs >= coalescingWindowMs){
                int currentIdx = nextIndexToUse.getAndIncrement();
                moduloIdx= getModuloIndex(currentIdx);
                lastIndexChangeInNs=currentNs;
            }
            else{
                moduloIdx=getModuloIndex(nextIndexToUse.get());
            }
        }
        return getOutboundSmallChannel(moduloIdx);
    }

    private OutboundMessagingConnection automaticIncrementStrategy(){
        int currentIdx = nextIndexToUse.getAndIncrement();
        int moduloIdx= getModuloIndex(currentIdx);
        return getOutboundSmallChannel(moduloIdx);
    }

    @VisibleForTesting
    public OutboundMessagingConnection getConnection(MessageOut msg)
    {
        if (msg.connectionType == null)
        {
            // optimize for the common path (the small message channel)
            if (Stage.GOSSIP != msg.getStage())
            {
                if(msg.serializedSize(smallMessageChannel.getTargetVersion()) < LARGE_MESSAGE_THRESHOLD){
                    //This is a small channel
                    if(optionalDisabled){
                        return smallMessageChannel;
                    }
                    else if(numOptionalOutboundChannels<=0)
                    {
                        return smallMessageChannel;
                    }

                    if(optionalOutboundChannelsStrategy.equalsIgnoreCase("FIXED_WINDOW")){
                        return fixedWindowStrategy();
                    }
                    else{
                        return automaticIncrementStrategy();
                    }
                }
                else{
                   return largeMessageChannel;
                }
            }
            return gossipChannel;
        }
        else
        {
            return getConnection(msg.connectionType);
        }
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param addr IP Address to use (and prefer) going forward for connecting to the peer
     */
    public void reconnectWithNewIp(InetAddressAndPort addr)
    {
        preferredRemoteAddr = addr;
        gossipChannel.reconnectWithNewIp(addr);
        largeMessageChannel.reconnectWithNewIp(addr);
        smallMessageChannel.reconnectWithNewIp(addr);
    }

    /**
     * Close each netty channel and it's socket.
     *
     * @param softClose {@code true} if existing messages in the queue should be sent before closing.
     */
    public void close(boolean softClose)
    {
        gossipChannel.close(softClose);
        largeMessageChannel.close(softClose);
        smallMessageChannel.close(softClose);
    }

    @VisibleForTesting
    final OutboundMessagingConnection getConnection(ConnectionType connectionType)
    {
        switch (connectionType)
        {
            case SMALL_MESSAGE:
                return smallMessageChannel;
            case LARGE_MESSAGE:
                return largeMessageChannel;
            case GOSSIP:
                return gossipChannel;
            default:
                throw new IllegalArgumentException("unsupported connection type: " + connectionType);
        }
    }

    public void incrementTimeout()
    {
        metrics.timeouts.mark();
    }

    public long getTimeouts()
    {
        return metrics.timeouts.getCount();
    }

    public InetAddressAndPort getPreferredRemoteAddr()
    {
        return preferredRemoteAddr;
    }
}
