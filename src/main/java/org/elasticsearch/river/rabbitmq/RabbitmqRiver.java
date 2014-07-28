/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.*;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RabbitmqRiver extends AbstractRiverComponent implements River {

    private final Address[] rabbitAddresses;
    private final String rabbitUser;
    private final String rabbitPassword;
    private final String rabbitVhost;

    private final String rabbitQueue;
    private final boolean rabbitQueueDeclare;
    private final boolean rabbitQueueBind;
    private final String rabbitExchange;
    private final String rabbitExchangeType;
    private final String rabbitRoutingKey;
    private final boolean rabbitExchangeDurable;
    private final boolean rabbitExchangeDeclare;
    private final boolean rabbitQueueDurable;
    private final boolean rabbitQueueAutoDelete;
    private Map rabbitQueueArgs = null; //extra arguments passed to queue for creation (ha settings for example)
    private final TimeValue rabbitHeartbeat;
    private final boolean rabbitNackErrors;

    private final int bulkSize;
    private final boolean ordered;
    private final ReplicationType replicationType;

    private volatile BulkProcessor bulkProcessor;
    private final RabbitMQBulkListener listener;
    private final Nacker nacker;

    private final ExecutableScript bulkScript;
    private final ExecutableScript script;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;
    private volatile Channel channel;

    @SuppressWarnings({"unchecked"})
    @Inject
    public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);

        if (settings.settings().containsKey("rabbitmq")) {
            Map<String, Object> rabbitSettings = (Map<String, Object>) settings.settings().get("rabbitmq");

            if (rabbitSettings.containsKey("addresses")) {
                List<Address> addresses = new ArrayList<Address>();
                for(Map<String, Object> address : (List<Map<String, Object>>) rabbitSettings.get("addresses")) {
                    addresses.add( new Address(XContentMapValues.nodeStringValue(address.get("host"), "localhost"),
                            XContentMapValues.nodeIntegerValue(address.get("port"), AMQP.PROTOCOL.PORT)));
                }
                rabbitAddresses = addresses.toArray(new Address[addresses.size()]);
            } else {
                String rabbitHost = XContentMapValues.nodeStringValue(rabbitSettings.get("host"), "localhost");
                int rabbitPort = XContentMapValues.nodeIntegerValue(rabbitSettings.get("port"), AMQP.PROTOCOL.PORT);
                rabbitAddresses = new Address[]{ new Address(rabbitHost, rabbitPort) };
            }

            rabbitUser = XContentMapValues.nodeStringValue(rabbitSettings.get("user"), "guest");
            rabbitPassword = XContentMapValues.nodeStringValue(rabbitSettings.get("pass"), "guest");
            rabbitVhost = XContentMapValues.nodeStringValue(rabbitSettings.get("vhost"), "/");

            rabbitQueue = XContentMapValues.nodeStringValue(rabbitSettings.get("queue"), "elasticsearch");
            rabbitExchange = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange"), "elasticsearch");
            rabbitRoutingKey = XContentMapValues.nodeStringValue(rabbitSettings.get("routing_key"), "elasticsearch");

            rabbitExchangeDeclare = XContentMapValues.nodeBooleanValue(rabbitSettings.get("exchange_declare"), true);
            if (rabbitExchangeDeclare) {
                
                rabbitExchangeType = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange_type"), "direct");
                rabbitExchangeDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("exchange_durable"), true);
            } else {
                rabbitExchangeType = "direct";
                rabbitExchangeDurable = true;
            }

            rabbitQueueDeclare = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_declare"), true);
            if (rabbitQueueDeclare) {
                rabbitQueueDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_durable"), true);
                rabbitQueueAutoDelete = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_auto_delete"), false);
                if (rabbitSettings.containsKey("args")) {
                    rabbitQueueArgs = (Map<String, Object>) rabbitSettings.get("args");
                }
            } else {
                rabbitQueueDurable = true;
                rabbitQueueAutoDelete = false;
            }
            rabbitQueueBind = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_bind"), true);

            rabbitHeartbeat = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    rabbitSettings.get("heartbeat"), "30m"), TimeValue.timeValueMinutes(30));
            rabbitNackErrors = XContentMapValues.nodeBooleanValue(rabbitSettings.get("nack_errors"), true);
        } else {
            rabbitAddresses = new Address[]{ new Address("localhost", AMQP.PROTOCOL.PORT) };
            rabbitUser = "guest";
            rabbitPassword = "guest";
            rabbitVhost = "/";

            rabbitQueue = "elasticsearch";
            rabbitQueueAutoDelete = false;
            rabbitQueueDurable = true;
            rabbitExchange = "elasticsearch";
            rabbitExchangeType = "direct";
            rabbitExchangeDurable = true;
            rabbitRoutingKey = "elasticsearch";

            rabbitExchangeDeclare = true;
            rabbitQueueDeclare = true;
            rabbitQueueBind = true;

            rabbitHeartbeat = TimeValue.timeValueMinutes(30);
            rabbitNackErrors = true;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
            replicationType = ReplicationType.fromString(XContentMapValues.nodeStringValue(indexSettings.get("replication"), "default"));
        } else {
            bulkSize = 100;
            ordered = false;
            replicationType = ReplicationType.DEFAULT;
        }

        nacker = new Nacker(rabbitNackErrors);

        listener = new RabbitMQBulkListener() {
            DateTime previousDate = DateTime.now();

            public void ackOrNack(Tuple<Nacker.Status, Long> nackerStatus) {
                if (nackerStatus.v1().equals(Nacker.Status.ACK)) {
                    // We need to ack
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("ack'ing delivery tag [{}]", nackerStatus.v2());
                        }
                        channel.basicAck(nackerStatus.v2(), false);
                    } catch (Exception e1) {
                        logger.warn("failed to ack [{}]", e1, nackerStatus.v2());
                    }
                } else if (nackerStatus.v1().equals(Nacker.Status.NACK)) {
                    // We need to ack
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("nack'ing delivery tag [{}]", nackerStatus.v2());
                        }
                        channel.basicNack(nackerStatus.v2(), false, false);
                    } catch (Exception e1) {
                        logger.warn("failed to nack [{}]", e1, nackerStatus.v2());
                    }
                }
            }

            public void ackPendingTags(boolean force) {
                // If we have more than one tag in the delivery tag list, it
                // means that we have finished dealing with the first ones
                // so we can ack them

                // If we did not ack anything for a long time, we can say that
                // we have finished to deal with latest delivery tag. So we can
                // force ack'ing remaining tags
                DateTime now = DateTime.now();
                if (Seconds.secondsBetween(previousDate, now).isGreaterThan(Seconds.seconds(5))) {
                    force = true;
                }
                if (force) {
                    previousDate = now;
                    logger.debug("force acking pending delivery/error tag if any");
                    ackOrNack(nacker.forceAck());
                }
            }

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                request.replicationType(replicationType);
                logger.debug("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.debug("Executed bulk composed of {} actions", request.numberOfActions());
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk", response.buildFailureMessage());
                }

                // We iterate over response
                Tuple<Nacker.Status, Long> nackerStatus;
                for (BulkItemResponse item : response.getItems()) {
                    Long payload = (Long) request.payloads().get(item.getItemId());
                    if (item.isFailed()) {
                        nackerStatus = nacker.addFailedTag(payload);
                        // TODO Put this single message in an error queue?
                        if (logger.isDebugEnabled()) {
                            logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                                    item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                        }
                    } else {
                        nackerStatus = nacker.addDeliveryTag(payload);
                    }

                    ackOrNack(nackerStatus);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
                // We iterate over
                Tuple<Nacker.Status, Long> nackerStatus;
                for (Object payload : request.payloads()) {
                    nackerStatus = nacker.addFailedTag((Long) payload);
                    ackOrNack(nackerStatus);
                }
            }
        };

        bulkProcessor = BulkProcessor.builder(client, listener)
                // TODO Replace with index.flush_interval: 5s
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                // TODO replace `index.ordered` boolean by `index.max_concurrent_bulk` integer
                .setConcurrentRequests(ordered ? 0 : 1)
                .setBulkActions(bulkSize)
                .build();
        
        if (settings.settings().containsKey("bulk_script_filter")) {
            Map<String, Object> scriptSettings = (Map<String, Object>) settings.settings().get("bulk_script_filter");
            if (scriptSettings.containsKey("script")) {
                String scriptLang = "native";
                if(scriptSettings.containsKey("script_lang")) {
                    scriptLang = scriptSettings.get("script_lang").toString();
                }
                Map<String, Object> scriptParams = null;
                if (scriptSettings.containsKey("script_params")) {
                    scriptParams = (Map<String, Object>) scriptSettings.get("script_params");
                } else {
                    scriptParams = Maps.newHashMap();
                }
                bulkScript = scriptService.executable(scriptLang, scriptSettings.get("script").toString(), scriptParams);
            } else {
                bulkScript = null;
            }
        } else {
          bulkScript = null;
        }

        if (settings.settings().containsKey("script_filter")) {
            Map<String, Object> scriptSettings = (Map<String, Object>) settings.settings().get("script_filter");
            if (scriptSettings.containsKey("script")) {
                String scriptLang = "mvel";
                if(scriptSettings.containsKey("script_lang")) {
                    scriptLang = scriptSettings.get("script_lang").toString();
                }
                Map<String, Object> scriptParams = null;
                if (scriptSettings.containsKey("script_params")) {
                    scriptParams = (Map<String, Object>) scriptSettings.get("script_params");
                } else {
                    scriptParams = Maps.newHashMap();
                }
                script = scriptService.executable(scriptLang, scriptSettings.get("script").toString(), scriptParams);
            } else {
                script = null;
            }
        } else {
            script = null;
        }

    }

    @Override
    public void start() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(rabbitUser);
        connectionFactory.setPassword(rabbitPassword);
        connectionFactory.setVirtualHost(rabbitVhost);
        connectionFactory.setRequestedHeartbeat(new Long(rabbitHeartbeat.getSeconds()).intValue());

        logger.info("creating rabbitmq river, addresses [{}], user [{}], vhost [{}]", rabbitAddresses, connectionFactory.getUsername(), connectionFactory.getVirtualHost());

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "rabbitmq_river").newThread(new Consumer());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        if (bulkProcessor != null) bulkProcessor.close();
        listener.ackPendingTags(true);

        logger.info("closing rabbitmq river");
        closed = true;

        if (thread != null) {
            thread.interrupt();
        }
    }

    private class Consumer implements Runnable {

        private Connection connection;

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    break;
                }
                try {
                    connection = connectionFactory.newConnection(rabbitAddresses);
                    channel = connection.createChannel();
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection / channel", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }

                QueueingConsumer consumer = new QueueingConsumer(channel);
                // define the queue
                try {
                    if (rabbitQueueDeclare) {
                        // only declare the queue if we should
                        channel.queueDeclare(rabbitQueue/*queue*/, rabbitQueueDurable/*durable*/, false/*exclusive*/, rabbitQueueAutoDelete/*autoDelete*/, rabbitQueueArgs/*extra args*/);
                    }
                    if (rabbitExchangeDeclare) {
                        // only declare the exchange if we should
                        channel.exchangeDeclare(rabbitExchange/*exchange*/, rabbitExchangeType/*type*/, rabbitExchangeDurable);
                    }
                    if (rabbitQueueBind) {
                        // only bind queue if we should
                        channel.queueBind(rabbitQueue/*queue*/, rabbitExchange/*exchange*/, rabbitRoutingKey/*routingKey*/);
                    }
                    channel.basicConsume(rabbitQueue/*queue*/, false/*noAck*/, consumer);
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create queue. Check your queue settings. Throttling river for 10s.");
                        // Print expected settings
                        if (rabbitQueueDeclare) {
                            logger.debug("expected settings: queue [{}], durable [{}], exclusive [{}], auto_delete [{}], args [{}]",
                                    rabbitQueue, rabbitQueueDurable, false, rabbitQueueAutoDelete, rabbitQueueArgs);
                        }
                        if (rabbitExchangeDeclare) {
                            logger.debug("expected settings: exchange [{}], type [{}], durable [{}]",
                                    rabbitExchange, rabbitExchangeType, rabbitExchangeDurable);
                        }
                        if (rabbitQueueBind) {
                            logger.debug("expected settings for queue binding: queue [{}], exchange [{}], routing_key [{}]",
                                    rabbitQueue, rabbitExchange, rabbitRoutingKey);
                        }

                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e1) {
                            // ignore, if we are closing, we will exit later
                        }
                    }
                    cleanup(0, "failed to create queue");
                    continue;
                }

                // now use the queue to listen for messages
                while (true) {
                    if (closed) {
                        break;
                    }
                    QueueingConsumer.Delivery task;
                    try {
                        // We listen to the queue for one second. If we don't have any message after one second,
                        // we will try to ack/nack pending delivery tags.
                        task = consumer.nextDelivery(1000);
                        // Ack'ing or Nack'ing any pending delivery tag. It's a very fast operation
                        // in most cases (when we don't have anything to ack/nack basically) so we
                        // almost immediately restart waiting for new incoming messages.
                        listener.ackPendingTags(false);
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to get next message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }

                    if (task != null && task.getBody() != null) {
                        try {
                            processBody(task.getBody(), task.getEnvelope().getDeliveryTag());
                        } catch (Exception e) {
                            logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                            try {
                                channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                            } catch (IOException e1) {
                                logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
                            }
                            continue;
                        }
                    }
                }
            }
            cleanup(0, "closing river");
        }

        private void cleanup(int code, String message) {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close(code, message);
                }
            } catch (Exception e) {
                logger.debug("failed to close channel on [{}]", e, message);
            }
            try {
                if (connection != null && connection.isOpen()) {
                    connection.close(code, message);
                }
            } catch (Exception e) {
                logger.debug("failed to close connection on [{}]", e, message);
            }
        }

        private void processBody(byte[] body, long deliveryTag) throws Exception {
            if (body == null) return;

            // first, the "full bulk" script
            if (bulkScript != null) {
                String bodyStr = new String(body);
                bulkScript.setNextVar("body", bodyStr);
                String newBodyStr = (String) bulkScript.run();
                if (newBodyStr == null) return ;
                body =  newBodyStr.getBytes();
            }

            // second, the "doc per doc" script
            if (script != null) {
                processBodyPerLine(body, deliveryTag);
            } else {
                bulkProcessor.add(new BytesArray(body), false, null, null, deliveryTag);
            }
        }

        private void processBodyPerLine(byte[] body, long deliveryTag) throws Exception {
            BufferedReader reader = new BufferedReader(new StringReader(new String(body)));

            JsonFactory factory = new JsonFactory();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                JsonXContentParser parser = new JsonXContentParser(factory.createJsonParser(line));
                Map<String, Object> asMap = parser.map();

                if (asMap.get("delete") != null) {
                    // We don't touch deleteRequests
                    String newContent = line + "\n";
                    bulkProcessor.add(new BytesArray(newContent.getBytes()), false, null, null, deliveryTag);
                } else {
                    // But we send other requests to the script Engine in ctx field
                    Map<String, Object> ctx;
                    String payload = null;
                    try {
                        payload = reader.readLine();
                        ctx = XContentFactory.xContent(XContentType.JSON).createParser(payload).mapAndClose();
                    } catch (IOException e) {
                        logger.warn("failed to parse {}", e, payload);
                        continue;
                    }
                    script.setNextVar("ctx", ctx);
                    script.run();
                    ctx = (Map<String, Object>) script.unwrap(ctx);
                    if (ctx != null) {
                        // Adding header
                        StringBuffer request = new StringBuffer(line);
                        request.append("\n");
                        // Adding new payload
                        request.append(XContentFactory.jsonBuilder().map(ctx).string());
                        request.append("\n");

                        if (logger.isTraceEnabled()) {
                            logger.trace("new bulk request is now: {}", request.toString());
                        }
                        byte[] binRequest = request.toString().getBytes();
                        bulkProcessor.add(new BytesArray(binRequest), false, null, null, deliveryTag);
                    }
                }
            }
        }
    }

    interface RabbitMQBulkListener extends BulkProcessor.Listener {
        public void ackPendingTags(boolean force);
    }
}
