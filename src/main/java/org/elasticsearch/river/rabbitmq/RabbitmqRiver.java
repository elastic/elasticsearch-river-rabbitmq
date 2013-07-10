/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.script.ScriptService;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RabbitmqRiver extends AbstractRiverComponent implements River {

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
    private Map rabbitQueueArgs = null; //extra arguments passed to queue for creation (ha settings for example)
    private final TimeValue rabbitHeartbeat;

    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    private final ExecutableScript bulkScript;
    private final ExecutableScript script;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;

    @SuppressWarnings({"unchecked"})
    @Inject
    public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client, ScriptService scriptService) {
        super(riverName, settings);
        this.client = client;

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
                    channel.basicConsume(rabbitQueue/*queue*/, false/*noAck*/, consumer);
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create queue [{}]", e, rabbitQueue);
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

                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                            try {
                                while ((task = consumer.nextDelivery(bulkTimeout.millis())) != null) {
                                    try {
                                        processBody(task.getBody(), bulkRequestBuilder);
                                        deliveryTags.add(task.getEnvelope().getDeliveryTag());
                                    } catch (Throwable e) {
                                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
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
                                if (bulkRequestBuilder.numberOfActions() > 0) {
                                  BulkResponse response = bulkRequestBuilder.execute().actionGet();
                                  if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                  }
                                }
                                for (Long deliveryTag : deliveryTags) {
                                    try {
                                        channel.basicAck(deliveryTag, false);
                                    } catch (Exception e1) {
                                        logger.warn("failed to ack [{}]", e1, deliveryTag);
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn("failed to execute bulk", e);
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
                                                channel.basicAck(deliveryTag, false);
                                            } catch (Exception e1) {
                                                logger.warn("failed to ack [{}]", e1, deliveryTag);
                                            }
                                        }
                                    }
                                    
                                    @Override
                                    public void onFailure(Throwable e) {
                                        logger.warn("failed to execute bulk for delivery tags [{}], not ack'ing", e, deliveryTags);
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
                channel.close(code, message);
            } catch (Exception e) {
                logger.debug("failed to close channel on [{}]", e, message);
            }
            try {
                connection.close(code, message);
            } catch (Exception e) {
                logger.debug("failed to close connection on [{}]", e, message);
            }
        }

        private void processBody(byte[] body, BulkRequestBuilder bulkRequestBuilder) throws Exception {
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
                processBodyPerLine(body, bulkRequestBuilder);
            } else {
                bulkRequestBuilder.add(body, 0, body.length, false);
            }
        }

        private void processBodyPerLine(byte[] body, BulkRequestBuilder bulkRequestBuilder) throws Exception {
            BufferedReader reader = new BufferedReader(new StringReader(new String(body)));

            JsonFactory factory = new JsonFactory();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                JsonXContentParser parser = new JsonXContentParser(factory.createJsonParser(line));
                Map<String, Object> asMap = parser.map();

                if (asMap.get("delete") != null) {
                    // We don't touch deleteRequests
                    String newContent = line + "\n";
                    bulkRequestBuilder.add(newContent.getBytes(), 0, newContent.getBytes().length, false);
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
                        bulkRequestBuilder.add(binRequest, 0, binRequest.length, false);
                    }
                }
            }
        }
    }
}
