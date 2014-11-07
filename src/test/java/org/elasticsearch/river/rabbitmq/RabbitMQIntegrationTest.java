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
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.rabbitmq.script.MockScriptFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.net.ConnectException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for RabbitMQ river<br>
 * You may have a rabbitmq instance running on localhost:15672.
 */
@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE,
        numDataNodes = 1,
        numClientNodes = 0,
        transportClientRatio = 0.0)
@AbstractRabbitMQTest.RabbitMQTest
public class RabbitMQIntegrationTest extends ElasticsearchIntegrationTest {

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

    private void launchTest(XContentBuilder river,
                            final int numMessages,
                            final int numDocsPerMessage,
                            InjectorHook injectorHook,
                            boolean delete,
                            boolean update
                            )
            throws Exception {

        final String dbName = getDbName();
        logger.info(" --> create index [{}]", dbName);
        try {
            client().admin().indices().prepareDelete(dbName).get();
        } catch (IndexMissingException e) {
            // No worries.
        }
        try {
            createIndex(dbName);
        } catch (IndexMissingException e) {
            // No worries.
        }
        ensureGreen(dbName);

        logger.info("  -> Checking rabbitmq running");
        // We try to connect to RabbitMQ.
        // If it's not launched, we don't fail the test but only log it
        Channel channel = null;
        Connection connection = null;
        try {
            logger.info(" --> connecting to rabbitmq");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(AMQP.PROTOCOL.PORT);
            connection = factory.newConnection();
        } catch (ConnectException ce) {
            throw new Exception("RabbitMQ service is not launched on localhost:" + AMQP.PROTOCOL.PORT +
                    ". Can not start Integration test. " +
                    "Launch `rabbitmq-server`.", ce);
        }

        try {
            logger.info("  -> Creating [{}] channel", dbName);
            channel = connection.createChannel();

            logger.info("  -> Creating queue [{}]", dbName);
            channel.queueDeclare(getDbName(), true, false, false, null);

            // We purge the queue in case of something is remaining there
            logger.info("  -> Purging [{}] channel", dbName);
            channel.queuePurge(getDbName());

            logger.info("  -> Put [{}] messages with [{}] documents each = [{}] docs", numMessages, numDocsPerMessage,
                    numMessages * numDocsPerMessage);
            final Set<String> removed = new HashSet<String>();
            int nbUpdated = 0;
            for (int i = 0; i < numMessages; i++) {
                StringBuffer message = new StringBuffer();

                for (int j = 0; j < numDocsPerMessage; j++) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("  -> Indexing document [{}] - [{}][{}]", i + "_" + j, i, j);
                    }
                    message.append("{ \"index\" : { \"_index\" : \"" +  dbName + "\", \"_type\" : \"typex\", \"_id\" : \""+ i + "_" + j +"\" } }\n");
                    message.append("{ \"field\" : \"" + i + "_" + j + "\",\"numeric\" : " + i * j + " }\n");

                    // Sometime we update a document
                    if (update && rarely()) {
                        String id = between(0, i) + "_" + between(0, j);
                        // We can only update if it has not been removed :)
                        if (!removed.contains(id)) {
                            logger.debug("  -> Updating document [{}] - [{}][{}]", id, i, j);
                            message.append("{ \"update\" : { \"_index\" : \"" + dbName + "\", \"_type\" : \"typex\", \"_id\" : \""+ id +"\" } }\n");
                            message.append("{ \"doc\": { \"foo\" : \"bar\", \"field2\" : \"" + i + "_" + j + "\" }}\n");
                            nbUpdated++;
                        }
                    }

                    // Sometime we delete a document
                    if (delete && rarely()) {
                        String id = between(0, i) + "_" + between(0, j);
                        if (!removed.contains(id)) {
                            logger.debug("  -> Removing document [{}] - [{}][{}]", id, i, j);
                            message.append("{ \"delete\" : { \"_index\" : \"" + dbName + "\", \"_type\" : \"typex\", \"_id\" : \""+ id +"\" } }\n");
                            removed.add(id);
                        }
                    }
                }

                channel.basicPublish("", dbName, null, message.toString().getBytes());
            }

            logger.info("  -> We removed [{}] docs and updated [{}] docs", removed.size(), nbUpdated);

            if (injectorHook != null) {
                logger.info("  -> Injecting extra data");
                injectorHook.inject();
            }

            logger.info(" --> create river");
            IndexResponse indexResponse = index("_river", dbName, "_meta", river);
            assertTrue(indexResponse.isCreated());

            logger.info("-->  checking that river [{}] was created", dbName);
            assertThat(awaitBusy(new Predicate<Object>() {
                public boolean apply(Object obj) {
                    GetResponse response = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, dbName, "_status").get();
                    return response.isExists();
                }
            }, 5, TimeUnit.SECONDS), equalTo(true));


            // Check that docs are still processed by the river
            logger.info(" --> waiting for expected number of docs: [{}]", numDocsPerMessage * numMessages - removed.size());
            assertThat(awaitBusy(new Predicate<Object>() {
                public boolean apply(Object obj) {
                    try {
                        refresh();
                        int expected = numDocsPerMessage * numMessages - removed.size();
                        CountResponse response = client().prepareCount(dbName).get();
                        logger.debug("  -> got {} docs, expected {}", response.getCount(), expected);
                        return response.getCount() == expected;
                    } catch (IndexMissingException e) {
                        return false;
                    }
                }
            }, 20, TimeUnit.SECONDS), equalTo(true));
        } finally {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }

            // Deletes the river
            GetResponse response = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, dbName, "_status").get();
            if (response.isExists()) {
                client().prepareDelete(RiverIndexName.Conf.DEFAULT_INDEX_NAME, dbName, "_meta").get();
                client().prepareDelete(RiverIndexName.Conf.DEFAULT_INDEX_NAME, dbName, "_status").get();
            }

            assertThat(awaitBusy(new Predicate<Object>() {
                public boolean apply(Object obj) {
                    GetResponse response = client().prepareGet(RiverIndexName.Conf.DEFAULT_INDEX_NAME, dbName, "_status").get();
                    return response.isExists();
                }
            }, 5, TimeUnit.SECONDS), equalTo(false));
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
                    .startObject("index")
                        .field("ordered", true)
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true);
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
                        .field("ordered", true)
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true);
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
                    .startObject("index")
                        .field("ordered", true)
                    .endObject()
                .endObject(), randomIntBetween(1, 10), randomIntBetween(1, 500), null, true, true);
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
                    .startObject("index")
                        .field("ordered", true)
                    .endObject()
                .endObject(), randomIntBetween(5, 20), randomIntBetween(100, 1000), null, false, false);
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
                        .field("script", " if (ctx.numeric != null) {ctx.numeric += param1}")
                        .startObject("script_params")
                            .field("param1", 1)
                        .endObject()
                    .endObject()
                    .startObject("index")
                        .field("ordered", true)
                    .endObject()
                .endObject(), 3, 10, null, true, true);

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
    public void testInlineScriptWithAdditionalInfos() throws Exception {
        launchTest(jsonBuilder()
                    .startObject()
                        .field("type", "rabbitmq")
                        .startObject("rabbitmq")
                            .field("queue", getDbName())
                        .endObject()
                        .startObject("script_filter")
                            .field("script", "ctx.bulkindextype = _index + '#' + _type; ctx.lengthid = (_id != null ? _id.length() : 0)")
                        .endObject()
                        .startObject("index")
                            .field("ordered", true)
                        .endObject()
                    .endObject(), 3, 10, null, true, true);

        // We should have data we don't have without raw set to true
        SearchResponse response = client().prepareSearch(getDbName())
                .addField("bulkindextype")
                .addField("lengthid")
                .get();

        logger.info("  --> Search response: {}", response.toString());

        for (SearchHit hit : response.getHits().getHits()) {
            assertThat(hit.field("bulkindextype"), notNullValue());
            assertThat(hit.field("bulkindextype").<String>getValue(), equalTo(hit.getIndex() + "#" + hit.getType()));

            assertThat(hit.field("lengthid"), notNullValue());
            assertThat(hit.field("lengthid").getValue(), instanceOf(Integer.class));
            assertThat(hit.field("lengthid").<Integer>getValue(), equalTo(hit.getId().length()));
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
                    .startObject("index")
                        .field("ordered", true)
                    .endObject()
                .endObject(), 3, 10, null, true, true);

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
                        .field("script", "if (ctx.numeric != null) {ctx.numeric += param1}")
                        .startObject("script_params")
                            .field("param1", 1)
                        .endObject()
                    .endObject()
                    .startObject("bulk_script_filter")
                        .field("script", "mock_script")
                        .field("script_lang", "native")
                    .endObject()
                    .startObject("index")
                        .field("ordered", true)
                    .endObject()
                .endObject(), 3, 10, null, true, true);

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
}
