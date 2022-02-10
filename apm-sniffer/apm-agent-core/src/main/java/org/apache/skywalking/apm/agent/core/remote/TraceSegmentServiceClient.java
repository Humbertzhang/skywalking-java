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

import com.google.gson.JsonObject;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import okhttp3.Response;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.MediaType;

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

import org.apache.skywalking.apm.agent.core.ml.lstm.GloveJudgeAnomaly;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.buffer.BufferStrategy;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.apm.network.common.v3.Commands;
import org.apache.skywalking.apm.network.language.agent.v3.SegmentObject;
import org.apache.skywalking.apm.network.language.agent.v3.TraceSegmentReportServiceGrpc;
import org.apache.skywalking.apm.network.logging.v3.LogData;

import static org.apache.skywalking.apm.agent.core.conf.Config.Buffer.BUFFER_SIZE;
import static org.apache.skywalking.apm.agent.core.conf.Config.Buffer.CHANNEL_SIZE;
import static org.apache.skywalking.apm.agent.core.remote.GRPCChannelStatus.CONNECTED;

import org.apache.skywalking.apm.agent.core.ml.lstm.JudgeAnomaly;

@DefaultImplementor
public class TraceSegmentServiceClient implements BootService, IConsumer<TraceSegment>, TracingContextListener, GRPCChannelListener {
    private static final ILog LOGGER = LogManager.getLogger(TraceSegmentServiceClient.class);

    private long lastLogTime;
    private long segmentUplinkedCounter;
    private long segmentAbandonedCounter;
    private volatile DataCarrier<TraceSegment> carrier;
    private volatile TraceSegmentReportServiceGrpc.TraceSegmentReportServiceStub serviceStub;
    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;

    // ML model
    GloveJudgeAnomaly judgement = new GloveJudgeAnomaly();


    // Start PostTraceWork Init Codes
    private Map<String, TraceSegment> postTraceMap = new HashMap<>();
    private String postTraceEnv = System.getenv("PostTrace");
    private int postTraceQueryInterval = this.getIntEnvWithDefault("PostTraceQueryInterval", 10);
    private int postTraceDeleteInterval = this.getIntEnvWithDefault("PostTraceDeleteInterval", 300);
    private String postTraceCenterURL = System.getenv("PostTraceCenterURL");

    // Request Body, Response Body and Http client
    private OkHttpClient okHttpClient = new OkHttpClient();
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    // class for PostTrace Query TraceIds
    public class TraceIdListResponse {
        public List<String> traceIds;
    }

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

    private void PostTraceQueryAndReport(Map<String, TraceSegment> postTraceMap) {
        // 1. Query Error TraceIds
        List<String> errorTraceIds = null;

        Request request = new Request.Builder()
                .url(this.postTraceCenterURL + "/traceids")
                .get()
                .build();

        try {
            Response response = okHttpClient.newCall(request).execute();
            if (response.isSuccessful()) {
                Gson gson = new Gson();
                ResponseBody responseBody = response.body();
                TraceIdListResponse traceIdList = gson.fromJson(responseBody.string(), TraceIdListResponse.class);
                errorTraceIds = traceIdList.traceIds;
                LOGGER.debug("query traceids successful, result is:" + traceIdList);
            }
        } catch (Exception e) {
            LOGGER.warn("Query traceIds error, exception is " + e.toString());
        }

        if (errorTraceIds == null) {
            LOGGER.warn("query traceIds error");
            return;
        }

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

        if (postTraceEnv != null) {
            // PostTrace Schedule PostTraceQueryAndReport Task
            POST_TRACE_SCHEDULE.scheduleAtFixedRate(
                    () -> this.PostTraceQueryAndReport(postTraceMap),
                    120,
                    postTraceQueryInterval,
                    TimeUnit.SECONDS
            );
        }
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
                            LOGGER.info("for segment in data");

                            if (checkSegmentReportable(segment)) {
                                SegmentObject upstreamSegment = segment.transform();
                                upstreamSegmentStreamObserver.onNext(upstreamSegment);

                                // Report Error TraceId to Center
                                JsonObject jsonObject = new JsonObject();
                                jsonObject.addProperty("trace_id", upstreamSegment.getTraceId());
                                String result = jsonObject.toString();
                                RequestBody body = RequestBody.create(JSON, result);
                                Request request = new Request.Builder()
                                        .url(this.postTraceCenterURL + "/traceid")
                                        .post(body)
                                        .build();
                                Response response = okHttpClient.newCall(request).execute();
                                if (response.isSuccessful()) {
                                    LOGGER.info("report trace_id " + upstreamSegment.getTraceId() + " to center successful");
                                } else {
                                    LOGGER.warn("report trace_id " + upstreamSegment.getTraceId() + " to center failed");
                                }

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
        LOGGER.info("checkSegmentReportable:" + segment.getTraceSegmentId());
        for (AbstractTracingSpan span : segment.getSpans()) {
            if (span.isErrorOccurred()) {
                return true;
            } else {
                List<String> logs = new ArrayList<>();
                LOGGER.info("span.logDataList.size():" + span.logDataList.size());
                LOGGER.info("span.logDataList:" + span.logDataList);
                for (LogData logData: span.logDataList) {
                    String log = logData.getBody().getText().getText();
                    logs.add(log);
                }
                LOGGER.info("start check judgement if is anomaly");
                if (logs.size() > 0 && judgement.isAnomaly(logs)) {
                    return true;
                }
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
