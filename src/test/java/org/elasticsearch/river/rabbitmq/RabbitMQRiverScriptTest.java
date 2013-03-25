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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.river.rabbitmq.script.MockScriptFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class RabbitMQRiverScriptTest {

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
          .put("gateway.type", "none")
          .put("index.number_of_shards", 1)
          .put("index.number_of_replicas", 0)
          .put("script.native.mock_script.type", MockScriptFactory.class)
          .build();
        Node node = NodeBuilder.nodeBuilder().settings(settings).node();

        node.client().prepareIndex("_river", "test1", "_meta").setSource(
            jsonBuilder().startObject()
              .field("type", "rabbitmq")
              .startObject("bulk_script_filter")
                .field("script", "mock_script")
                .field("script_lang", "native")
            .endObject()).execute().actionGet();

        ConnectionFactory cfconn = new ConnectionFactory();
        cfconn.setHost("localhost");
        cfconn.setPort(AMQP.PROTOCOL.PORT);
        Connection conn = cfconn.newConnection();

        Channel ch = conn.createChannel();
        ch.exchangeDeclare("elasticsearch", "direct", true);
        ch.queueDeclare("elasticsearch", true, false, false, null);

        String message =
                "{ \"index\" :  { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" } }\n" +
                "{ \"type1\" :  { \"field1\" : \"value1\" } }\n" +
                "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
                "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"3\" } }\n" +
                "{ \"type1\" :  { \"field3\" : \"value3\" } }" +
                "";

        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());

        ch.close();
        conn.close();

        Thread.sleep(100000);
    }
}
