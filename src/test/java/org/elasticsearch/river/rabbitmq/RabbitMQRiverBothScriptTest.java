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

import com.rabbitmq.client.Channel;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.node.Node;
import org.elasticsearch.river.rabbitmq.script.MockScriptFactory;
import org.junit.Assert;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class RabbitMQRiverBothScriptTest extends RabbitMQTestRunner {

    @Override
    protected void pushMessages(Channel ch) throws IOException {
        String message =
                "{ \"index\" :  { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
                        "{ \"type1\" :  { \"field1\" : 1 } }\n" +
                        "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
                        "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
                        "{ \"type1\" :  { \"field1\" : 2 } }" +
                        "";

        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
    }

    @Override
    protected XContentBuilder river() throws IOException {
        return  jsonBuilder()
                .startObject()
                    .field("type", "rabbitmq")
                    .startObject("script_filter")
                        .field("script", "ctx.type1.field1 += param1")
                        .field("script_lang", "mvel")
                        .startObject("script_params")
                            .field("param1", 1)
                        .endObject()
                    .endObject()
                    .startObject("bulk_script_filter")
                        .field("script", "mock_script")
                        .field("script_lang", "native")
                    .endObject()
                .endObject();
    }

    @Override
    protected long expectedDocuments() {
        return 1;
    }

    @Override
    protected Settings nodeSettings() {
        return ImmutableSettings.settingsBuilder().put("script.native.mock_script.type", MockScriptFactory.class).build();
    }

    @Override
    protected void postInjectionTests(Node node) {
        super.postInjectionTests(node);

        // Doc 1 should exist
        GetResponse getResponse = node.client().prepareGet("test", "type1", "1").execute().actionGet();
        Assert.assertNotNull(getResponse);
        Assert.assertTrue(getResponse.isExists());
        Assert.assertNotNull(getResponse.getSourceAsMap());
        Assert.assertEquals(2, XContentMapValues.extractValue("type1.field1", getResponse.getSourceAsMap()));

        // Doc 3 should not exist
        getResponse = node.client().prepareGet("test", "type1", "3").execute().actionGet();
        Assert.assertNotNull(getResponse);
        Assert.assertFalse(getResponse.isExists());
    }
}
