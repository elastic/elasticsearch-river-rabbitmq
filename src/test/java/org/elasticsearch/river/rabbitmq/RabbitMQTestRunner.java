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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;

/**
 *
 */
@AbstractRabbitMQTest.RabbitMQTest
public abstract class RabbitMQTestRunner extends ElasticsearchIntegrationTest {

    protected ESLogger logger = ESLoggerFactory.getLogger(RabbitMQTestRunner.class.getName());
    private static String INDEX = "test";

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
     */
    protected void postInjectionTests() {
        logger.info(" --> checking number of documents");
        CountResponse response = client().prepareCount("test").execute().actionGet();
        // We have consumed all messages. We can now check expected number of documents
        Assert.assertEquals("Wrong number of documents found", expectedDocuments(), response.getCount());
    }

    @Test
    public void test_all_messages_are_consumed() throws Exception {

        // We try to connect to RabbitMQ.
        // If it's not launched, we don't fail the test but only log it
        try {
            logger.info(" --> remove existing indices");
            wipeIndices("_all");

            logger.info(" --> connecting to rabbitmq");
            ConnectionFactory cfconn = new ConnectionFactory();
            cfconn.setHost("localhost");
            cfconn.setPort(AMQP.PROTOCOL.PORT);
            Connection conn = cfconn.newConnection();

            Channel ch = conn.createChannel();
            ch.exchangeDeclare("elasticsearch", "direct", true);
            AMQP.Queue.DeclareOk queue = ch.queueDeclare("elasticsearch", true, false, false, null);

            // We purge the queue in case of something is remaining there
            ch.queuePurge("elasticsearch");

            logger.info(" --> sending messages");
            pushMessages(ch);

            logger.info(" --> create river");
            createIndex(INDEX);

            index("_river", "test", "_meta", river());

            // We need at some point to check if we have consumed the river
            int steps = timeout();
            long count = 0;

            while (true) {
                // We wait for one second
                Thread.sleep(1000);

                CountResponse response = client().prepareCount("test").execute().actionGet();
                count = response.getCount();

                steps--;
                if (steps < 0 || count == expectedDocuments()) {
                    break;
                }
            }

            ch.close();
            conn.close();

            postInjectionTests();
        } catch (ConnectException e) {
            throw new Exception("RabbitMQ service is not launched on localhost:" +AMQP.PROTOCOL.PORT + ". Can not start Integration test. " +
                    "Launch `rabbitmq-server`.", e);
        }
    }
}
