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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;

/**
 *
 */
public abstract class RabbitMQTestRunner {

    protected ESLogger logger = ESLoggerFactory.getLogger(RabbitMQTestRunner.class.getName());
    private static String INDEX = "test";

    protected Node node;

    protected abstract void pushMessages(Channel ch) throws IOException;

    protected abstract XContentBuilder river() throws IOException;

    protected abstract long expectedDocuments();

    /**
     * We don't want to run test more than needed. In
     * case of timeout, test should fail as it means we did not
     * consume all the queue
     * @return Timeout for test (default to 10s)
     */
    protected int timeout() {
        return 10;
    }

    /**
     * Helper method to inject x messages containing y index requests
     * @param ch RabbitMQ channel
     * @param nbMessages number of messages to inject (x)
     * @param nbDocs number of docs per message to inject (y)
     * @throws IOException
     */
    protected static void pushMessages(Channel ch, int nbMessages, int nbDocs) throws IOException {
        for (int i = 0; i < nbMessages; i++) {
            StringBuffer message = new StringBuffer();
            for (int j = 0; j < nbDocs; j++) {
                message.append("{ \"index\" : { \"_index\" : \""+INDEX+"\", \"_type\" : \"typex\", \"_id\" : \""+ i + "_" + j +"\" } }\n");
                message.append("{ \"typex\" : { \"field\" : \"" + i + "_" + j + "\" } }\n");
            }
            ch.basicPublish("elasticsearch", "elasticsearch", null, message.toString().getBytes());
        }
    }

    /**
     * If you need to run specific tests, just override this method.
     * By default, we check the number of expected documents
     * @param node Elasticsearch current node
     */
    protected void postInjectionTests(Node node) {
        CountResponse response = node.client().prepareCount("test").execute().actionGet();
        // We have consumed all messages. We can now check expected number of documents
        Assert.assertEquals("Wrong number of documents found", expectedDocuments(), response.getCount());
    }

    /**
     * Override if you need to add specific settings for your node
     * @return
     */
    protected Settings nodeSettings() {
        return ImmutableSettings.settingsBuilder().build();
    }

    @Test
    public void test_all_messages_are_consumed() throws Exception {

        // We try to connect to RabbitMQ.
        // If it's not launched, we don't fail the test but only log it
        try {
            ConnectionFactory cfconn = new ConnectionFactory();
            cfconn.setHost("localhost");
            cfconn.setPort(AMQP.PROTOCOL.PORT);
            Connection conn = cfconn.newConnection();

            Channel ch = conn.createChannel();
            ch.exchangeDeclare("elasticsearch", "direct", true);
            AMQP.Queue.DeclareOk queue = ch.queueDeclare("elasticsearch", true, false, false, null);

            // We purge the queue in case of something is remaining there
            ch.queuePurge("elasticsearch");

            pushMessages(ch);

            // We can now create our node and our river
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("gateway.type", "none")
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(nodeSettings())
                .build();
            node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();

            // We first remove existing index if any
            try {
                node.client().admin().indices().prepareDelete(INDEX).execute().actionGet();
            } catch (IndexMissingException e) {
                // Index is missing? It's perfectly fine!
            }

            // Let's create an index for our docs and we will disable refresh
            node.client().admin().indices().prepareCreate(INDEX).execute().actionGet();

            node.client().prepareIndex("_river", "test", "_meta").setSource(river()).execute().actionGet();

            // We need at some point to check if we have consumed the river
            int steps = timeout();
            long count = 0;

            while (true) {
                // We wait for one second
                Thread.sleep(1000);

                CountResponse response = node.client().prepareCount("test").execute().actionGet();
                count = response.getCount();

                steps--;
                if (steps < 0 || count == expectedDocuments()) {
                    break;
                }
            }

            ch.close();
            conn.close();

            postInjectionTests(node);
        } catch (ConnectException e) {
            logger.warn("RabbitMQ service is not launched on localhost:{}. Can not start Integration test. " +
                    "Launch `rabbitmq-server`.", AMQP.PROTOCOL.PORT);
        }
    }

    @After
    public void tearDown() {
        // After each test, we kill the node
        if (node != null) {
            node.close();
        }
        node = null;
    }
}
