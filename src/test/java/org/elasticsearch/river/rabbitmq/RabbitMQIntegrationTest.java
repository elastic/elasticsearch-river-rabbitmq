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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.rabbitmq.helper.HttpClient;
import org.elasticsearch.river.rabbitmq.helper.HttpClientResponse;
import org.elasticsearch.river.rabbitmq.script.MockScriptFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for RabbitMQ river<br>
 * You may have a rabbitmq instance running on localhost with default guest/guest login/pwd.
 * AMQP should run under 5672 port and HTTP port under 15672.
 */
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 1,
        numClientNodes = 0,
        transportClientRatio = 0.0)
@AbstractRabbitMQTest.RabbitMQTest
public class RabbitMQIntegrationTest extends ElasticsearchIntegrationTest {

    public final static String rabbitmq_hostname = ConnectionFactory.DEFAULT_HOST;
    public final static int rabbitmq_port = ConnectionFactory.DEFAULT_AMQP_PORT;
    public final static int rabbitmq_httpport = 10000 + rabbitmq_port;
    public final static String rabbitmq_login = ConnectionFactory.DEFAULT_USER;
    public final static String rabbitmq_password = ConnectionFactory.DEFAULT_PASS;
    public final static String rabbitmq_vhost = "%2F";

    private interface InjectorHook {
        public void inject();
    }

    private static final String testDbPrefix = "elasticsearch_test_";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("script.native.mock_script.type", MockScriptFactory.class)
                .put("threadpool.bulk.queue_size", 200)
                .build();
    }

    private String getDbName() {
        String testName = testDbPrefix.concat(Strings.toUnderscoreCase(getTestName()));
        return testName.indexOf(" ") >= 0? Strings.split(testName, " ")[0] : testName;
    }

    private Connection connectToRabbitMQ() throws IOException {
        logger.info(" --> connecting to rabbitmq");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmq_hostname);
        factory.setPort(rabbitmq_port);
        return factory.newConnection();
    }

    private Channel openChannel(Connection connection) throws IOException {
        logger.info("  -> Creating [{}] channel", getDbName());
        Channel channel = connection.createChannel();

        logger.info("  -> Creating queue [{}]", getDbName());
        channel.queueDeclare(getDbName(), true, false, false, null);

        return channel;
    }

    private void closeRabbitMQ(Connection connection, Channel channel) throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    private long getRemainingMessages(String queue) {
        HttpClient httpClient = new HttpClient(RabbitMQIntegrationTest.rabbitmq_hostname, RabbitMQIntegrationTest.rabbitmq_httpport);
        Map<String, String> headers = Maps.newHashMap();
        headers.put("Authorization", "Basic " + Base64.encodeBytes((RabbitMQIntegrationTest.rabbitmq_login + ":" + RabbitMQIntegrationTest.rabbitmq_password).getBytes()));

        HttpClientResponse response = httpClient.request("GET", "/api/queues/" + rabbitmq_vhost + "/" + queue, headers, null);

        String regex = ".*\"messages\":([0-9]*).*";
        Pattern pattern = Regex.compile(regex, null);

        Matcher matcher = pattern.matcher(response.response());
        matcher.find();
        long nbMessages = Long.parseLong(matcher.group(1));

        return nbMessages;
    }

    /**
     * Rarely returns <code>true</code> in about 1 per thousand of all calls.
     */
    private static boolean veryRarely() {
        return randomInt(1000) >= 999;
    }

    private void launchTest(XContentBuilder river,
                            final int numMessages,
                            final int numDocsPerMessage,
                            InjectorHook injectorHook,
                            boolean delete,
                            boolean update,
                            boolean errors
                            )
            throws Exception {
        logger.info("  -> Checking rabbitmq running");
        // We try to connect to RabbitMQ.
        // If it's not launched, we don't fail the test but only log it
        Channel channel = null;
        Connection connection = null;
        try {
            connection = connectToRabbitMQ();
            channel = openChannel(connection);

            // We purge the queue in case of something is remaining there
            logger.info("  -> Purging [{}] channel", getDbName());
            channel.queuePurge(getDbName());

            logger.info("  -> Put [{}] messages with [{}] documents each = [{}] docs", numMessages, numDocsPerMessage,
                    numMessages * numDocsPerMessage);
            final Set<String> removed = new HashSet<String>();
            int nbUpdated = 0;
            for (int i = 0; i < numMessages; i++) {
                StringBuffer message = new StringBuffer();

                for (int j = 0; j < numDocsPerMessage; j++) {
                    message.append("{ \"index\" : { \"_index\" : \""+getDbName()+"\", \"_type\" : \"typex\", \"_id\" : \""+ i + "_" + j +"\" } }\n");
                    message.append("{ \"field\" : \"" + i + "_" + j + "\",\"numeric\" : " + i * j + " }\n");

                    // Sometime we update a document
                    if (update && rarely()) {
                        String id = between(0, i) + "_" + between(0, j);
                        // We can only update if it has not been removed :)
                        if (!removed.contains(id)) {
                            logger.trace("  -> Updating message [{}] - [{}][{}]", id, i, j);
                            message.append("{ \"update\" : { \"_index\" : \""+getDbName()+"\", \"_type\" : \"typex\", \"_id\" : \""+ id +"\" } }\n");
                            message.append("{ \"doc\": { \"foo\" : \"bar\", \"field2\" : \"" + i + "_" + j + "\" }}\n");
                            nbUpdated++;
                        }
                    }

                    // Sometime we delete a document
                    if (delete && rarely()) {
                        String id = between(0, i) + "_" + between(0, j);
                        if (!removed.contains(id)) {
                            logger.trace("  -> Removing message [{}] - [{}][{}]", id, i, j);
                            message.append("{ \"delete\" : { \"_index\" : \""+getDbName()+"\", \"_type\" : \"typex\", \"_id\" : \""+ id +"\" } }\n");
                            removed.add(id);
                        }
                    }

                    // Sometime we introduce a bad document
                    if (errors && veryRarely()) {
                        logger.trace("  -> Adding error message at [{}][{}]", i, j);
                        message.append("{ \"index\" : { \"_index\" : \""+getDbName()+"\", \"_type\" : \"typex\", \"_id\" : \""+ i + "_" + j +"\" } }\n");
                        message.append("{ \"field\" : \"" + i + "_" + j + "\",\"numeric\" : \"stringvalue\" }\n");
                    }
                }

                channel.basicPublish("", getDbName(), null, message.toString().getBytes());
            }

            logger.info("  -> We removed [{}] docs and updated [{}] docs", removed.size(), nbUpdated);

            if (injectorHook != null) {
                logger.info("  -> Injecting extra data");
                injectorHook.inject();
            }

            logger.info(" --> create index [{}]", getDbName());
            try {
                createIndex(getDbName());
            } catch (IndexAlreadyExistsException e) {
                // No worries. We already created the index before
            }

            logger.info(" --> create river");
            index("_river", getDbName(), "_meta", river);

            // Check that docs are still processed by the river
            logger.info(" --> waiting for expected number of docs: [{}]", numDocsPerMessage * numMessages - removed.size());
            assertThat(awaitBusy(new Predicate<Object>() {
                public boolean apply(Object obj) {
                    try {
                        refresh();
                        CountResponse response = client().prepareCount(getDbName()).get();
                        logger.debug("  -> got {} docs", response.getCount());
                        return response.getCount() == numDocsPerMessage * numMessages - removed.size();
                    } catch (IndexMissingException e) {
                        return false;
                    }
                }
            }, 20, TimeUnit.SECONDS), equalTo(true));

            // We need to check that nothing is remaining in RabbitMQ queue
            if (numDocsPerMessage * numMessages > 0) {
                assertThat("No non ack'ed/nack'ed message should remain in RabbitMQ.", awaitBusy(new Predicate<Object>() {
                    public boolean apply(Object obj) {
                    long remainingMessages = getRemainingMessages(getDbName());
                    logger.debug("  -> got {} remaining non acked/nacked messages", remainingMessages);
                    return remainingMessages == 0L;
                    }
                }, 30, TimeUnit.SECONDS), equalTo(true));

            }

        } catch (Throwable e) {
            throw new Exception("RabbitMQ service is not launched on localhost:" + AMQP.PROTOCOL.PORT +
                    ". Can not start Integration test. " +
                    "Launch `rabbitmq-server`.", e);
        } finally {
            closeRabbitMQ(connection, channel);
        }
    }

    @Test @Repeat(iterations = 10)
    public void testSimpleRiver() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true, false);
    }

    @Test
    public void testBulkProcessor() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                    .startObject("script_filter")
                        .field("script", "ctx.numeric += param1")
                        .startObject("script_params")
                            .field("param1", 1)
                        .endObject()
                    .endObject()
                    .startObject("index")
                        .field("bulk_size", 10)
                    .endObject()
                .endObject(), 10, 100, null, true, true, false);
    }

    @Test @LuceneTestCase.Slow
    public void testALotOfData() throws Exception {
        final int numMessages = between(100, 1000);
        int totalMessages = 0;

        logger.info("  -> going to inject [{}] bulks", numMessages);

        launchTest(jsonBuilder()
                .startObject()
                .field("type", "rabbitmq")
                .startObject("rabbitmq")
                    .field("queue", getDbName())
                .endObject()
                .startObject("index")
                    .field("bulk_size", between(100, 1500))
                .endObject()
                .endObject(), 0, 0, null, false, false, false);

        // We reinject some data from here
        Connection connection = connectToRabbitMQ();
        Channel channel = openChannel(connection);

        try {
            for (int i = 0; i < numMessages; i++) {
                StringBuffer message = new StringBuffer();
                int numDocsPerMessage = between(100, 1000);
                totalMessages += numDocsPerMessage;

                logger.info("  -> bulk [{}] adding [{}] docs. Total injected by now: [{}]", i, numDocsPerMessage, totalMessages);

                for (int j = 0; j < numDocsPerMessage; j++) {
                    message.append("{ \"index\" : { \"_index\" : \""+getDbName()+"\", \"_type\" : \"typex\", \"_id\" : \""+ i + "_" + j +"\" } }\n");
                    message.append("{ \"field\" : \"" + i + "_" + j + "\",\"numeric\" : " + i * j + " }\n");
                }

                channel.basicPublish("", getDbName(), null, message.toString().getBytes());
            }
        } finally {
            closeRabbitMQ(connection, channel);
        }

        // We wait for results
        final int finalTotalMessages = totalMessages;
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    CountResponse response = client().prepareCount(getDbName()).get();
                    logger.debug("  -> got {} docs", response.getCount());
                    return response.getCount() == finalTotalMessages;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 10, TimeUnit.MINUTES), equalTo(true));

    }

    @Test
    public void testAsyncReplication() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                    .startObject("index")
                        .field("replication", "async")
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true, false);
    }

    @Test
    public void testHeartbeat() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                        .field("heartbeat", "100ms")
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true, false);
    }

    @Test
    public void testConsumers() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                        .field("num_consumers", 5)
                    .endObject()
                .endObject(), randomIntBetween(5, 20), randomIntBetween(100, 1000), null, false, false, false);
    }

    @Test
    public void testInlineScript() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                    .startObject("script_filter")
                        .field("script", "ctx.numeric += param1")
                        .startObject("script_params")
                            .field("param1", 1)
                        .endObject()
                    .endObject()
                .endObject(), 3, 10, null, true, true, false);

        // We should have data we don't have without raw set to true
        SearchResponse response = client().prepareSearch(getDbName())
                .addField("numeric")
                .get();

        logger.info("  --> Search response: {}", response.toString());

        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.field("numeric"), notNullValue());
            assertThat(hit.field("numeric").getValue(), instanceOf(Integer.class));
            // Value is based on id
            String[] id = Strings.split(hit.getId(), "_");
            int expected = Integer.parseInt(id[0]) * Integer.parseInt(id[1]) + 1;
            assertThat((Integer) hit.field("numeric").getValue(), is(expected));
        }

    }

    @Test
    public void testNativeScript() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                    .startObject("bulk_script_filter")
                        .field("script", "mock_script")
                        .field("script_lang", "native")
                    .endObject()
                .endObject(), 3, 10, null, true, true, false);

        // We should have data we don't have without raw set to true
        SearchResponse response = client().prepareSearch(getDbName())
                .addField("numeric")
                .get();

        logger.info("  --> Search response: {}", response.toString());

        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.field("numeric"), notNullValue());
            assertThat(hit.field("numeric").getValue(), instanceOf(Integer.class));
            // Value is based on id
            String[] id = Strings.split(hit.getId(), "_");
            int expected = Integer.parseInt(id[0]) * Integer.parseInt(id[1]) + 1;
            assertThat((Integer) hit.field("numeric").getValue(), is(expected));
        }
    }

    @Test
    public void testBothScript() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                    .startObject("script_filter")
                        .field("script", "ctx.numeric += param1")
                        .startObject("script_params")
                            .field("param1", 1)
                        .endObject()
                    .endObject()
                    .startObject("bulk_script_filter")
                        .field("script", "mock_script")
                        .field("script_lang", "native")
                    .endObject()
                .endObject(), 3, 10, null, true, true, false);

        // We should have data we don't have without raw set to true
        SearchResponse response = client().prepareSearch(getDbName())
                .addField("numeric")
                .get();

        logger.info("  --> Search response: {}", response.toString());

        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.field("numeric"), notNullValue());
            assertThat(hit.field("numeric").getValue(), instanceOf(Integer.class));
            // Value is based on id
            String[] id = Strings.split(hit.getId(), "_");
            int expected = Integer.parseInt(id[0]) * Integer.parseInt(id[1]) + 2;
            assertThat((Integer) hit.field("numeric").getValue(), is(expected));
        }
    }

    @Test
    public void testWithSimulatedErrorsWithNackOn() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true, true);

        // We should have data we don't have without raw set to true
        SearchResponse response = client().prepareSearch(getDbName())
                .addField("numeric")
                .get();

        logger.info("  --> Search response: {}", response.toString());

        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.field("numeric"), notNullValue());
            assertThat(hit.field("numeric").getValue(), instanceOf(Integer.class));
            // Value is based on id
            String[] id = Strings.split(hit.getId(), "_");
            int expected = Integer.parseInt(id[0]) * Integer.parseInt(id[1]);
            assertThat((Integer) hit.field("numeric").getValue(), is(expected));
        }
    }

    @Test
    public void testWithSimulatedErrorsWithNackOff() throws Exception {
        launchTest(jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("rabbitmq")
                        .field("queue", getDbName())
                        .field("nack_errors", false)
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true, true);

        // We should have data we don't have without raw set to true
        SearchResponse response = client().prepareSearch(getDbName())
                .addField("numeric")
                .get();

        logger.info("  --> Search response: {}", response.toString());

        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.field("numeric"), notNullValue());
            assertThat(hit.field("numeric").getValue(), instanceOf(Integer.class));
            // Value is based on id
            String[] id = Strings.split(hit.getId(), "_");
            int expected = Integer.parseInt(id[0]) * Integer.parseInt(id[1]);
            assertThat((Integer) hit.field("numeric").getValue(), is(expected));
        }
    }
}
