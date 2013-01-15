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

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class RabbitMQRiverTest {

    public static void main(String[] args) throws Exception {

        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none").put("cluster.name", "es-mqtest")).node();

        node.client().prepareIndex("_river", "mqtest1", "_meta").setSource(
        		jsonBuilder().startObject().field("type", "rabbitmq")
        		.startObject("rabbitmq").field("host", "rabbit-qa1").endObject()
        		.endObject()
        		).execute().actionGet();

        ConnectionFactory cfconn = new ConnectionFactory();
        cfconn.setHost("rabbit-qa1");
        cfconn.setPort(AMQP.PROTOCOL.PORT);
        Connection conn = cfconn.newConnection();

        Channel ch = conn.createChannel();
        ch.exchangeDeclare("elasticsearch", "direct", true);
        ch.queueDeclare("elasticsearch", true, false, false, null);

        Thread.sleep(5000);
        String message = "{ \"index\" : { \"_index\" : \"mqtest\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
                "{ \"delete\" : { \"_index\" : \"mqtest\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
                "{ \"create\" : { \"_index\" : \"mqtest\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }";


        String mapping = "{ \"type2\" : { \"properties\" : {\"data\" : {\"dynamic\" : true,\"properties\" : {\"myString\" : {\"type\" : \"string\",\"boost\" : 1.0,\"index\" : \"not_analyzed\",\"store\" : \"no\"},\"myText\" : {\"type\" : \"string\",\"include_in_all\" : true,\"index\" : \"analyzed\",\"store\" : \"no\"}}}}}}";
        String mappingMessage = "{ \"_index\" : \"mqtest\", \"_type\" : \"type2\"}\n" +
        						mapping;
        String partialmappingMessage = "{ \"_index\" : \"mqtest\", \"_type\" : \"type2\"}";
        
        HashMap<String,Object> headers = new HashMap<String, Object>();
        headers.put("X-ES-Command", "mapping");
        BasicProperties props = MessageProperties.MINIMAL_BASIC;
        props = props.builder().headers(headers).build();
        ch.basicPublish("elasticsearch", "elasticsearch", props, mappingMessage.getBytes());

        Thread.sleep(5000);
        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());
        ch.close();
        conn.close();

        Thread.sleep(5000);
        Boolean exists = node.client().get(new GetRequest("mqtest").id("1")).get().exists();
        ClusterState state = node.client().admin().cluster().state(new ClusterStateRequest().filteredIndices("mqtest")).get().state();
        ImmutableMap<String, MappingMetaData>  mappings = state.getMetaData().index("mqtest").mappings();
        MappingMetaData typeMap = mappings.get("type2");
        if (null != typeMap){
        	String gotMapping = typeMap.source().toString();
        }
    }
}
