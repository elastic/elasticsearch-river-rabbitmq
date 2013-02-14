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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
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
	private final String rabbitExchange;
	private final String rabbitExchangeType;
	private final String rabbitRoutingKey;
	private final boolean rabbitExchangeDurable;
	private final boolean rabbitQueueDurable;
	private final boolean rabbitQueueAutoDelete;
	private final int rabbitNumPrefetch;
	private Map rabbitQueueArgs = null; // extra arguments passed to queue for
										// creation (ha settings for example)

	private final int bulkSize;
	private final TimeValue bulkTimeout;
	private final boolean ordered;
	private final boolean warnOnBulkErrors;

	private volatile boolean closed = false;

	private volatile Thread thread;

	private volatile ConnectionFactory connectionFactory;

	@SuppressWarnings({ "unchecked" })
	@Inject
	public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client) {
		super(riverName, settings);
		this.client = client;

		if (settings.settings().containsKey("rabbitmq")) {
			Map<String, Object> rabbitSettings = (Map<String, Object>) settings.settings().get("rabbitmq");

			if (rabbitSettings.containsKey("addresses")) {
				List<Address> addresses = new ArrayList<Address>();
				for (Map<String, Object> address : (List<Map<String, Object>>) rabbitSettings.get("addresses")) {
					addresses.add(new Address(XContentMapValues.nodeStringValue(address.get("host"), "localhost"), XContentMapValues.nodeIntegerValue(address.get("port"), AMQP.PROTOCOL.PORT)));
				}
				rabbitAddresses = addresses.toArray(new Address[addresses.size()]);
			} else {
				String rabbitHost = XContentMapValues.nodeStringValue(rabbitSettings.get("host"), "localhost");
				int rabbitPort = XContentMapValues.nodeIntegerValue(rabbitSettings.get("port"), AMQP.PROTOCOL.PORT);
				rabbitAddresses = new Address[] { new Address(rabbitHost, rabbitPort) };
			}

			rabbitUser = XContentMapValues.nodeStringValue(rabbitSettings.get("user"), "guest");
			rabbitPassword = XContentMapValues.nodeStringValue(rabbitSettings.get("pass"), "guest");
			rabbitVhost = XContentMapValues.nodeStringValue(rabbitSettings.get("vhost"), "/");

			rabbitQueue = XContentMapValues.nodeStringValue(rabbitSettings.get("queue"), "elasticsearch");
			rabbitExchange = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange"), "elasticsearch");
			rabbitExchangeType = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange_type"), "direct");
			rabbitRoutingKey = XContentMapValues.nodeStringValue(rabbitSettings.get("routing_key"), "elasticsearch");
			rabbitExchangeDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("exchange_durable"), true);
			rabbitQueueDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_durable"), true);
			rabbitQueueAutoDelete = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_auto_delete"), false);
			rabbitNumPrefetch = XContentMapValues.nodeIntegerValue(rabbitSettings.get("queue_prefetch"), 0);

			if (rabbitSettings.containsKey("args")) {
				rabbitQueueArgs = (Map<String, Object>) rabbitSettings.get("args");
			}
		} else {
			rabbitAddresses = new Address[] { new Address("localhost", AMQP.PROTOCOL.PORT) };
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
			rabbitNumPrefetch = 0;
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
			warnOnBulkErrors = XContentMapValues.nodeBooleanValue(indexSettings.get("warnOnBulkErrors"), true);
		} else {
			bulkSize = 100;
			bulkTimeout = TimeValue.timeValueMillis(10);
			ordered = false;
			warnOnBulkErrors = true;
		}
	}

	@Override
	public void start() {
		connectionFactory = new ConnectionFactory();
		connectionFactory.setUsername(rabbitUser);
		connectionFactory.setPassword(rabbitPassword);
		connectionFactory.setVirtualHost(rabbitVhost);

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
					if (rabbitNumPrefetch > 0)
						channel.basicQos(rabbitNumPrefetch);
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
					channel.exchangeDeclare(rabbitExchange/* exchange */, rabbitExchangeType/* type */, rabbitExchangeDurable);
					channel.queueDeclare(rabbitQueue/* queue */, rabbitQueueDurable/* durable */, false/* exclusive */, rabbitQueueAutoDelete/* autoDelete */, rabbitQueueArgs/*
																																											 * extra
																																											 * args
																																											 */);
					channel.queueBind(rabbitQueue/* queue */, rabbitExchange/* exchange */, rabbitRoutingKey/* routingKey */);
					channel.basicConsume(rabbitQueue/* queue */, false/* noAck */, consumer);
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

						// handle the first task. if we fail, don't continue
						// with this batch cycle
						if (!handleTask(task, deliveryTags, bulkRequestBuilder))
							continue;

						if (bulkRequestBuilder.numberOfActions() < bulkSize) {
							// try and spin some more of those without timeout,
							// so we have a bigger bulk (bounded by the bulk
							// size)
							try {
								while ((task = consumer.nextDelivery(bulkTimeout.millis())) != null) {
									handleTask(task, deliveryTags, bulkRequestBuilder);
									if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
										break;
									}
								}
							} catch (InterruptedException e) {
								if (closed) {
									break;
								}
							}
						}

						if (logger.isTraceEnabled()) {
							logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
						}

						if (ordered || bulkRequestBuilder.numberOfActions() == 0) {
							try {
								if (bulkRequestBuilder.numberOfActions() > 0) {
									BulkResponse response = bulkRequestBuilder.execute().actionGet();
									if (response.hasFailures()) {
										// TODO write to exception queue?
										if (warnOnBulkErrors)
											logger.warn("failed to execute some - " + response.buildFailureMessage());
										else
											logger.debug("failed to execute some - " + response.buildFailureMessage());
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
							bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
								@Override
								public void onResponse(BulkResponse response) {
									if (response.hasFailures()) {
										// TODO write to exception queue?
										if (warnOnBulkErrors)
											logger.warn("failed to execute some - " + response.buildFailureMessage());
										else
											logger.debug("failed to execute some - " + response.buildFailureMessage());
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
			cleanup(0, "closing river");
		}

		private boolean handleTask(QueueingConsumer.Delivery task, final List<Long> deliveryTags, BulkRequestBuilder bulkRequestBuilder) {
			// check for custom commands
			String customCommand = null;
			Map<String, Object> headers = task.getProperties().getHeaders();
			if (null != headers) {
				Object headerVal = headers.get("X-ES-Command");
				if (null != headerVal)
					customCommand = headerVal.toString();
			}

			// no custom command - batch request
			if (null == customCommand || customCommand.isEmpty()) {
				try {
					bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
					deliveryTags.add(task.getEnvelope().getDeliveryTag());
					return true;
				} catch (Exception e) {
					logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
					try {
						channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
					} catch (Exception e1) {
						logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
					}
					return false;
				}
				// mapping request
			} else if (customCommand.equalsIgnoreCase("mapping")) {
				try {
					CommandParser parser = null;
					try {
						parser = new CommandParser(task.getBody());
						PutMappingResponse response = client.admin().indices().preparePutMapping(parser.getIndex()).setType(parser.getType()).setSource(parser.content).execute().actionGet();
					} catch (IndexMissingException im) {
						// if the index has not been created yet, we can should
						// it with this mapping
						logger.trace("index {} is missing, creating with mappin", parser.getIndex());
						CreateIndexResponse res = client.admin().indices().prepareCreate(parser.getIndex()).addMapping(parser.getType(), parser.content).execute().actionGet();
					}

				} catch (Exception e) {
					logger.warn("failed to update mapping for delivery tag [{}]", e, task.getEnvelope().getDeliveryTag());
				}
				finally{
					try {
						channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
					} catch (Exception e1) {
						logger.warn("failed to ack on [{}]", e1, task.getEnvelope().getDeliveryTag());
					}
				}
				return true;
			} else if (customCommand.equalsIgnoreCase("deleteByQuery")) {
				try {
					CommandParser parser = null;
					parser = new CommandParser(task.getBody());
					if (null != parser.getIndex()) {
						DeleteByQueryRequest dreq = new DeleteByQueryRequest();
						dreq.indices(parser.getIndex());
						if (null != parser.getType())
							dreq.types(parser.getType());
						if (null != parser.queryString)
							dreq.query(new QueryStringQueryBuilder(parser.queryString));
						else
							dreq.query(parser.content);
						DeleteByQueryResponse response = client.deleteByQuery(dreq).actionGet();
					}
				} catch (Exception e) {
					logger.warn("failed to delete by query for delivery tag [{}]", e, task.getEnvelope().getDeliveryTag());
				}
				finally{
					try {
						channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
					} catch (Exception e1) {
						logger.warn("failed to ack on [{}]", e1, task.getEnvelope().getDeliveryTag());
					}
				}
				return true;
			} else {
				logger.warn("unknown custom command - {} [{}], ack'ing...", customCommand, task.getEnvelope().getDeliveryTag());
				try {
					channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
				} catch (Exception e1) {
					logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
				}
				return false;
			}
		}

		class CommandParser {
			private String index = null;
			private String type = null;
			private String queryString = null;
			private String content = null;

			public CommandParser(byte[] data) throws Exception {
				BytesArray arr = new BytesArray(data, 0, data.length);
				parse(arr);
			}

			private void parse(BytesReference data) throws Exception {
				XContent xContent = XContentFactory.xContent(data);
				String source = XContentBuilder.builder(xContent).string();
				int from = 0;
				int length = data.length();
				byte marker = xContent.streamSeparator();
				int nextMarker = findNextMarker(marker, from, data, length);
				if (nextMarker == -1) {
					nextMarker = length;
				}
				// now parse the action
				XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from));

				try {
					// move pointers
					from = nextMarker + 1;

					// Move to START_OBJECT
					XContentParser.Token token = parser.nextToken();
					if (token == null) {
						throw new Exception("Wrong object structure");
					}
					assert token == XContentParser.Token.START_OBJECT;
					// Move to FIELD_NAME, that's the action
					// token = parser.nextToken();
					// assert token == XContentParser.Token.FIELD_NAME;
					// String action = parser.currentName();

					String id = null;
					String routing = null;
					String parent = null;
					String timestamp = null;
					Long ttl = null;
					String opType = null;
					long version = 0;
					VersionType versionType = VersionType.INTERNAL;
					String percolate = null;

					// at this stage, next token can either be END_OBJECT
					// (and use default index and type, with auto generated
					// id)
					// or START_OBJECT which will have another set of
					// parameters

					String currentFieldName = null;
					while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
						if (token == XContentParser.Token.FIELD_NAME) {
							currentFieldName = parser.currentName();
						} else if (token.isValue()) {
							if ("_index".equals(currentFieldName)) {
								index = parser.text();
							} else if ("_type".equals(currentFieldName)) {
								type = parser.text();
							} else if ("_queryString".equals(currentFieldName)) {
								queryString = parser.text();
							} else if ("_id".equals(currentFieldName)) {
								id = parser.text();
							} else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
								routing = parser.text();
							} else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
								parent = parser.text();
							} else if ("_timestamp".equals(currentFieldName) || "timestamp".equals(currentFieldName)) {
								timestamp = parser.text();
							} else if ("_ttl".equals(currentFieldName) || "ttl".equals(currentFieldName)) {
								if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
									ttl = TimeValue.parseTimeValue(parser.text(), null).millis();
								} else {
									ttl = parser.longValue();
								}
							} else if ("op_type".equals(currentFieldName) || "opType".equals(currentFieldName)) {
								opType = parser.text();
							} else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
								version = parser.longValue();
							} else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName)
									|| "versionType".equals(currentFieldName)) {
								versionType = VersionType.fromString(parser.text());
							} else if ("percolate".equals(currentFieldName) || "_percolate".equals(currentFieldName)) {
								percolate = parser.textOrNull();
							}
						}
					}
					if (nextMarker < length) {
						nextMarker = findNextMarker(marker, from, data, length);
						if (nextMarker == -1) {
							nextMarker = length;
						}
						content = getString(data.slice(from, nextMarker - from));
					}

				} finally {
					parser.close();
				}

			}

			private int findNextMarker(byte marker, int from, BytesReference data, int length) {
				for (int i = from; i < length; i++) {
					if (data.get(i) == marker) {
						return i;
					}
				}
				return -1;
			}

			String getString(BytesReference data) throws IOException {
				return new String(data.array(), data.arrayOffset(), data.length(), Charsets.UTF_8);
			}

			String getIndex() {
				return index;
			}

			String getType() {
				return type;
			}

			String getQueryString() {
				return queryString;
			}

			String getContent() {
				return content;
			}

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
	}
}
