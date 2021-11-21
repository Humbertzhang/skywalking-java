/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.remote;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.commands.CommandService;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.context.TracingContextListener;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractTracingSpan;
import org.apache.skywalking.apm.agent.core.context.trace.TraceSegment;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.buffer.BufferStrategy;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.apm.network.common.v3.Commands;
import org.apache.skywalking.apm.network.language.agent.v3.SegmentObject;
import org.apache.skywalking.apm.network.language.agent.v3.TraceSegmentReportServiceGrpc;

import static org.apache.skywalking.apm.agent.core.conf.Config.Buffer.BUFFER_SIZE;
import static org.apache.skywalking.apm.agent.core.conf.Config.Buffer.CHANNEL_SIZE;
import static org.apache.skywalking.apm.agent.core.remote.GRPCChannelStatus.CONNECTED;

@DefaultImplementor
public class TraceSegmentServiceClient implements BootService, IConsumer<TraceSegment>, TracingContextListener, GRPCChannelListener {
    private static final ILog LOGGER = LogManager.getLogger(TraceSegmentServiceClient.class);

    private long lastLogTime;
    private long segmentUplinkedCounter;
    private long segmentAbandonedCounter;
    private volatile DataCarrier<TraceSegment> carrier;
    private volatile TraceSegmentReportServiceGrpc.TraceSegmentReportServiceStub serviceStub;
    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;

    // Start PostTraceWork Init Codes
    private Map<String, TraceSegment> postTraceMap = new HashMap<>();
    private String postTraceEnv = System.getenv("PostTrace");
    private int postTraceQueryInterval = this.getIntEnvWithDefault("PostTraceQueryInterval", 10);
    private int postTraceDeleteInterval = this.getIntEnvWithDefault("PostTraceDeleteInterval", 300);
    private String postTraceCenterURL = System.getenv("PostTraceCenterURL");

    private final static ScheduledExecutorService POST_TRACE_SCHEDULE = Executors.newSingleThreadScheduledExecutor(
            new DefaultNamedThreadFactory("POST_TRACE_SCHEDULE"));

    private int getIntEnvWithDefault(String key, int defaultValue) {
        int result;
        try {
            result = Integer.parseInt(System.getenv(key));
        } catch (NumberFormatException e) {
            result = defaultValue;
        }
        return result;
    }

    private void PostTraceQueryAndReport(String centerURL, Map<String, TraceSegment> postTraceMap) {
        // 1. Query Error TraceIds
        // TODO: Real query traceId
        List<String> errorTraceIds = new ArrayList<>();
        errorTraceIds.add("111222333");

        // 2. Send Error Segments
        Set<String> traceIdKeySet = postTraceMap.keySet();
        for (String traceId: errorTraceIds) {
            if (traceIdKeySet.contains(traceId)) {
                TraceSegment segment = postTraceMap.get(traceId);
                List<TraceSegment> data = new ArrayList<>();
                data.add(segment);
                data.add(null);

                // a hack method is used for sending PostTrace Error Segment in consume.
                this.consume(data);
            }
        }
    }

    private void deleteTraceInTraceMap(String traceId, Map<String, TraceSegment> postTraceMap) {
        postTraceMap.remove(traceId);
    }

    // End PostTraceWork Init Codes

    @Override
    public void prepare() {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);
    }

    @Override
    public void boot() {
        lastLogTime = System.currentTimeMillis();
        segmentUplinkedCounter = 0;
        segmentAbandonedCounter = 0;
        carrier = new DataCarrier<>(CHANNEL_SIZE, BUFFER_SIZE, BufferStrategy.IF_POSSIBLE);
        carrier.consume(this, 1);

        // PostTrace Schedule PostTraceQueryAndReport Task
        POST_TRACE_SCHEDULE.scheduleAtFixedRate(
                () -> this.PostTraceQueryAndReport(postTraceCenterURL, postTraceMap),
                120,
                postTraceQueryInterval,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() {
        TracingContext.ListenerManager.add(this);
    }

    @Override
    public void shutdown() {
        TracingContext.ListenerManager.remove(this);
        carrier.shutdownConsumers();
    }

    @Override
    public void init() {

    }

    @Override
    public void consume(List<TraceSegment> data) {
        if (CONNECTED.equals(status)) {
            final GRPCStreamServiceStatus status = new GRPCStreamServiceStatus(false);
            StreamObserver<SegmentObject> upstreamSegmentStreamObserver = serviceStub.withDeadlineAfter(
                Config.Collector.GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
            ).collect(new StreamObserver<Commands>() {
                @Override
                public void onNext(Commands commands) {
                    ServiceManager.INSTANCE.findService(CommandService.class)
                                           .receiveCommand(commands);
                }

                @Override
                public void onError(
                    Throwable throwable) {
                    status.finished();
                    if (LOGGER.isErrorEnable()) {
                        LOGGER.error(
                            throwable,
                            "Send UpstreamSegment to collector fail with a grpc internal exception."
                        );
                    }
                    ServiceManager.INSTANCE
                        .findService(GRPCChannelManager.class)
                        .reportError(throwable);
                }

                @Override
                public void onCompleted() {
                    status.finished();
                }
            });

            try {
                // insert h10g code here

                // PostTrace is active
                if (postTraceEnv != null) {
                    boolean sentSegment = false;

                    // A hack method for PostTrace.
                    // If the last element of data is Null, means it is PostTrace Send Error Segment Requests
                    // then just send it without check
                    if (data.size() > 1 & data.get(data.size() - 1) == null) {
                        // remove last null data
                        data.remove(data.size() - 1);

                        // send error segment
                        for (TraceSegment segment : data) {
                            SegmentObject upstreamSegment = segment.transform();
                            upstreamSegmentStreamObserver.onNext(upstreamSegment);
                        }
                        // set flag, so that do not check segment.
                        sentSegment = true;
                    }

                    if (sentSegment) {
                        LOGGER.info("PostTrace module has sent error Segment to OAP Server");
                    } else {
                        LOGGER.info("check if segment is error");
                        for (TraceSegment segment: data) {
                            if (checkSegmentReportable(segment)) {
                                SegmentObject upstreamSegment = segment.transform();
                                upstreamSegmentStreamObserver.onNext(upstreamSegment);
                            } else {
                                // Put Not Error TraceSegment in postTraceMap.
                                this.postTraceMap.put(segment.getRelatedGlobalTrace().getId(), segment);

                                // Delete Normal TraceSegment in fixed time
                                POST_TRACE_SCHEDULE.schedule(
                                        () -> this.deleteTraceInTraceMap(segment.getRelatedGlobalTrace().getId(), postTraceMap),
                                        this.postTraceDeleteInterval,
                                        TimeUnit.SECONDS
                                );
                            }
                        }
                    }
                } else {
                    // PostTrace is not active.
                    for (TraceSegment segment : data) {
                        SegmentObject upstreamSegment = segment.transform();
                        upstreamSegmentStreamObserver.onNext(upstreamSegment);
                    }
                }
            } catch (Throwable t) {
                LOGGER.error(t, "Transform and send UpstreamSegment to collector fail.");
            }

            upstreamSegmentStreamObserver.onCompleted();

            status.wait4Finish();
            segmentUplinkedCounter += data.size();
        } else {
            segmentAbandonedCounter += data.size();
        }

        printUplinkStatus();
    }

    private boolean checkSegmentReportable(TraceSegment segment) {
        for (AbstractTracingSpan span : segment.getSpans()) {
            if (span.isErrorOccurred()) {
                return true;
            }
        }
        return false;
    }

    private void printUplinkStatus() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastLogTime > 30 * 1000) {
            lastLogTime = currentTimeMillis;
            if (segmentUplinkedCounter > 0) {
                LOGGER.debug("{} trace segments have been sent to collector.", segmentUplinkedCounter);
                segmentUplinkedCounter = 0;
            }
            if (segmentAbandonedCounter > 0) {
                LOGGER.debug(
                    "{} trace segments have been abandoned, cause by no available channel.", segmentAbandonedCounter);
                segmentAbandonedCounter = 0;
            }
        }
    }

    @Override
    public void onError(List<TraceSegment> data, Throwable t) {
        LOGGER.error(t, "Try to send {} trace segments to collector, with unexpected exception.", data.size());
    }

    @Override
    public void onExit() {

    }

    @Override
    public void afterFinished(TraceSegment traceSegment) {
        if (traceSegment.isIgnore()) {
            return;
        }
        if (!carrier.produce(traceSegment)) {
            if (LOGGER.isDebugEnable()) {
                LOGGER.debug("One trace segment has been abandoned, cause by buffer is full.");
            }
        }
    }

    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            serviceStub = TraceSegmentReportServiceGrpc.newStub(channel);
        }
        this.status = status;
    }
}
