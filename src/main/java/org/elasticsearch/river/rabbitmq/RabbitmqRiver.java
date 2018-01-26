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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.jackson.core.JsonFactory;
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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class RabbitmqRiver extends AbstractRiverComponent implements River {

    private static final Map<String, String> AUTHORIZED_SCRIPT_VARS;

    private final Client client;

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
    private final int rabbitQosPrefetchSize;
    private final int rabbitQosPrefetchCount;
    private Map rabbitQueueArgs = null; //extra arguments passed to queue for creation (ha settings for example)
    private final TimeValue rabbitHeartbeat;
    private final boolean rabbitNackErrors;

    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    private final ScriptService scriptService;
    private final ExecutableScript bulkScript;
    private final ExecutableScript script;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;

    static {
        AUTHORIZED_SCRIPT_VARS = new HashMap<String, String>();
        AUTHORIZED_SCRIPT_VARS.put("_index", "_index");
        AUTHORIZED_SCRIPT_VARS.put("_type", "_type");
        AUTHORIZED_SCRIPT_VARS.put("_id", "_id");
        AUTHORIZED_SCRIPT_VARS.put("_version", "_version");
        AUTHORIZED_SCRIPT_VARS.put("version", "_version");
        AUTHORIZED_SCRIPT_VARS.put("_routing", "_routing");
        AUTHORIZED_SCRIPT_VARS.put("routing", "_routing");
        AUTHORIZED_SCRIPT_VARS.put("_parent", "_parent");
        AUTHORIZED_SCRIPT_VARS.put("parent", "_parent");
        AUTHORIZED_SCRIPT_VARS.put("_timestamp", "_timestamp");
        AUTHORIZED_SCRIPT_VARS.put("timestamp", "_timestamp");
        AUTHORIZED_SCRIPT_VARS.put("_ttl", "_ttl");
        AUTHORIZED_SCRIPT_VARS.put("ttl", "_ttl");
    }

    @SuppressWarnings({"unchecked"})
    @Inject
    public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;
        this.scriptService = scriptService;

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
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            ordered = false;
        }

        if (settings.settings().containsKey("rabbitmq")) {
            Map<String, Object> rabbitSettings = (Map<String, Object>) settings.settings().get("rabbitmq");
            rabbitQosPrefetchSize = XContentMapValues.nodeIntegerValue(rabbitSettings.get("qos_prefetch_size"), 0);
            rabbitQosPrefetchCount = XContentMapValues.nodeIntegerValue(rabbitSettings.get("qos_prefetch_count"), bulkSize * 2);
        } else {
            rabbitQosPrefetchSize = 0;
            rabbitQosPrefetchCount = bulkSize * 2;
        }

        bulkScript = buildScript("bulk_script_filter");
        script = buildScript("script_filter");
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
        logger.info("closing rabbitmq river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        private Connection connection;

        private Channel channel;

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
                    channel.basicQos(rabbitQosPrefetchSize/*qos_prefetch_size*/, rabbitQosPrefetchCount/*qos_prefetch_count*/, false);
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
                        task = consumer.nextDelivery();
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to get next message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }

                    if (task != null && task.getBody() != null) {
                        final List<Long> deliveryTags = Lists.newArrayList();
                        final List<String> theTasks = Lists.newArrayList();

                        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                        try {
                            processBody(task.getBody(), bulkRequestBuilder);
                        } catch (Exception e) {
                            logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                            try {
                                channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                            } catch (IOException e1) {
                                logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
                            }
                            continue;
                        }

                        deliveryTags.add(task.getEnvelope().getDeliveryTag());
                        theTasks.add(new String(task.getBody()));

                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                            try {
                                while ((task = consumer.nextDelivery(bulkTimeout.millis())) != null) {
                                    try {
                                        processBody(task.getBody(), bulkRequestBuilder);
                                        deliveryTags.add(task.getEnvelope().getDeliveryTag());
                                        theTasks.add(new String(task.getBody()));
                                    } catch (Throwable e) {
                                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e);
                                        try {
                                            channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                                        } catch (Exception e1) {
                                            logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
                                        }
                                    }
                                    if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
                                        break;
                                    }
                                }
                            } catch (InterruptedException e) {
                                if (closed) {
                                    break;
                                }
                            } catch (ShutdownSignalException sse) {
                                logger.warn("Received a shutdown signal! initiatedByApplication: [{}], hard error: [{}]", sse,
                                        sse.isInitiatedByApplication(), sse.isHardError());
                                if (!closed && sse.isInitiatedByApplication()) {
                                    logger.error("failed to get next message, reconnecting...", sse);
                                }
                                cleanup(0, "failed to get message");
                                break;
                            }
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                        }

                        if (ordered) {
                            try {
                                Boolean isRecoverableFailure = false;
                                if (bulkRequestBuilder.numberOfActions() > 0) {
                                  BulkResponse response = bulkRequestBuilder.execute().actionGet();
                                  if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                      String failMsg = response.buildFailureMessage()
                                      isRecoverableFailure = failMsg.contains("EsRejectedExecutionException");
                                      logger.warn("failed to execute" + failMsg);
                                  }
                                }
                                for (Long deliveryTag : deliveryTags) {
                                    try {
                                        if (isRecoverableFailure) {
                                            channel.basicNack(deliveryTag, false, false);
                                        } else {
                                            channel.basicAck(deliveryTag, false);
                                        }
                                    } catch (Exception e1) {
                                        logger.warn("failed to ack [{}]", e1, deliveryTag);
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn("failed to execute bulk", e);
                                if (rabbitNackErrors) {
                                    String taskBodies = "";
                                    for (String tb: theTasks) {
                                        taskBodies += tb;
                                    }
                                    logger.warn("failed to execute bulk for delivery tags [{}], nack'ing" + taskBodies, e, deliveryTags);
                                    for (Long deliveryTag : deliveryTags) {
                                        try {
                                            channel.basicNack(deliveryTag, false, false);
                                        } catch (Exception e1) {
                                            logger.warn("failed to nack [{}]", e1, deliveryTag);
                                        }
                                    }
                                } else {
                                    logger.warn("failed to execute bulk for delivery tags [{}], ignoring", e, deliveryTags);
                                }
                            }
                        } else {
                            if (bulkRequestBuilder.numberOfActions()>0) {
                                bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                                    @Override
                                    public void onResponse(BulkResponse response) {
                                        if (response.hasFailures()) {
                                            // TODO write to exception queue?
                                            logger.warn("failed to execute" + response.buildFailureMessage());
                                        }
                                        for (Long deliveryTag : deliveryTags) {
                                            try {
                                            } catch (Exception e1) {
                                                logger.warn("failed to ack [{}]", e1, deliveryTag);
                                            }
                                        }
                                    }

                                    @Override
                                    public void onFailure(Throwable e) {
                                        if (rabbitNackErrors) {
                                            logger.warn("failed to execute bulk for delivery tags [{}], nack'ing", e, deliveryTags);
                                            for (Long deliveryTag : deliveryTags) {
                                                try {
                                                    channel.basicNack(deliveryTag, false, false);
                                                } catch (Exception e1) {
                                                    logger.warn("failed to nack [{}]", e1, deliveryTag);
                                                }
                                            }
                                        } else {
                                            logger.warn("failed to execute bulk for delivery tags [{}], ignoring", e, deliveryTags);
                                        }
                                    }
                                });
                            }
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

        private void processBody(byte[] body, BulkRequestBuilder bulkRequestBuilder) throws Exception {
            if (body == null) return;

            // first, the "full bulk" script
            if (bulkScript != null) {
                String bodyStr = new String(body, StandardCharsets.UTF_8);
                bulkScript.setNextVar("body", bodyStr);
                String newBodyStr = (String) bulkScript.run();
                if (newBodyStr == null) return ;
                body =  newBodyStr.getBytes(StandardCharsets.UTF_8);
            }

            // second, the "doc per doc" script
            if (script != null) {
                processBodyPerLine(body, bulkRequestBuilder);
            } else {
                bulkRequestBuilder.add(body, 0, body.length);
            }
        }

        private void processBodyPerLine(byte[] body, BulkRequestBuilder bulkRequestBuilder) throws Exception {
            BufferedReader reader = new BufferedReader(new FastStringReader(new String(body, StandardCharsets.UTF_8)));

            JsonFactory factory = new JsonFactory();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                JsonXContentParser parser = new JsonXContentParser(factory.createParser(line));
                Map<String, Object> asMap = parser.map();

                if (asMap.get("delete") != null) {
                    // We don't touch deleteRequests
                    String newContent = line + "\n";
                    bulkRequestBuilder.add(newContent.getBytes(StandardCharsets.UTF_8), 0, newContent.getBytes(StandardCharsets.UTF_8).length);
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

                    // Sets some vars
                    script.setNextVar("ctx", ctx);

                    if (!asMap.isEmpty()) {
                        for (Map.Entry<String, Object> bulkItem : asMap.entrySet()) {
                            String action = bulkItem.getKey().toLowerCase(Locale.ROOT);
                            if ("index".equals(action) || "update".equals(action) || "create".equals(action)) {
                                script.setNextVar("_action", action);

                                Object bulkData = bulkItem.getValue();
                                if ((bulkData != null) && (bulkData instanceof Map)) {
                                    Map bulkItemMap = ((Map) bulkData);
                                    for(Object dataKey : bulkItemMap.keySet()) {
                                        if (AUTHORIZED_SCRIPT_VARS.containsKey(dataKey)) {
                                            script.setNextVar(AUTHORIZED_SCRIPT_VARS.get(dataKey), bulkItemMap.get(dataKey));
                                        }
                                    }
                                }
                            }
                        }
                    }

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
                        byte[] binRequest = request.toString().getBytes(StandardCharsets.UTF_8);
                        bulkRequestBuilder.add(binRequest, 0, binRequest.length);
                    }
                }
            }
        }
    }

    /**
     * Build an executable script if provided as settings
     * @param settingName
     * @return
     */
    private ExecutableScript buildScript(String settingName) {
        if (settings.settings().containsKey(settingName)) {
            Map<String, Object> scriptSettings = (Map<String, Object>) settings.settings().get(settingName);
            if (scriptSettings.containsKey("script")) {
                String scriptLang = "groovy";
                if (scriptSettings.containsKey("script_lang")) {
                    scriptLang = scriptSettings.get("script_lang").toString();
                }
                Map<String, Object> scriptParams = null;
                if (scriptSettings.containsKey("script_params")) {
                    scriptParams = (Map<String, Object>) scriptSettings.get("script_params");
                } else {
                    scriptParams = Maps.newHashMap();
                }
                return scriptService.executable(
                        new Script(scriptLang, scriptSettings.get("script").toString(), ScriptService.ScriptType.INLINE, scriptParams),
                        ScriptContext.Standard.UPDATE);
            }
        }

        return null;
    }
}
