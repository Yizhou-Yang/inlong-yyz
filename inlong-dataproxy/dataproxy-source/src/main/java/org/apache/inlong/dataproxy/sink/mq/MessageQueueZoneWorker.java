/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.sink.mq;

import org.apache.flume.lifecycle.LifecycleState;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.utils.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageQueueZoneWorker
 */
public class MessageQueueZoneWorker extends Thread {

    public static final Logger LOG = LoggerFactory.getLogger(MessageQueueZoneWorker.class);

    private final String workerName;
    private final MessageQueueZoneSinkContext context;

    private MessageQueueZoneProducer zoneProducer;
    private LifecycleState status;

    /**
     * Constructor
     */
    public MessageQueueZoneWorker(String sinkName, int workerIndex, MessageQueueZoneSinkContext context,
            MessageQueueZoneProducer zoneProducer) {
        super();
        this.workerName = sinkName + "-worker-" + workerIndex;
        this.context = context;
        this.zoneProducer = zoneProducer;
        this.status = LifecycleState.IDLE;
    }

    /**
     * start
     */
    @Override
    public void start() {
        this.status = LifecycleState.START;
        super.start();
    }

    /**
     * close
     */
    public void close() {
        // close all producers
        this.zoneProducer.close();
        this.status = LifecycleState.STOP;
    }

    /**
     * run
     */
    @Override
    public void run() {
        LOG.info(String.format("start MessageQueueZoneWorker:%s", this.workerName));
        while (status != LifecycleState.STOP) {
            BatchPackProfile event = null;
            try {
                event = context.getDispatchQueue().pollRecord();
                if (event == null) {
                    this.sleepOneInterval();
                    continue;
                }
                // send
                this.zoneProducer.send(event);
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
                if (event != null) {
                    dealWithFailedEvent(event);
                }
                this.sleepOneInterval();
            }
        }
    }

    /**
     * When send event failed, try to put back to dispatch queue or drop event based on different situation.
     *
     * @param event BatchPackProfile failed to send
     */
    private void dealWithFailedEvent(BatchPackProfile event) {
        ConfigManager configManager = ConfigManager.getInstance();
        String groupId = event.getInlongGroupId();
        String streamId = event.getInlongStreamId();
        String configTopic = MessageUtils.getTopic(
                configManager.getTopicProperties(), groupId, streamId);
        if (configTopic != null) {
            context.getDispatchQueue().offer(event);
        }
    }

    /**
     * sleepOneInterval
     */
    private void sleepOneInterval() {
        try {
            Thread.sleep(context.getProcessInterval());
        } catch (InterruptedException e1) {
            LOG.error(e1.getMessage(), e1);
        }
    }
}
