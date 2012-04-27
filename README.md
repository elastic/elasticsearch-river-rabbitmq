RabbitMQ River Plugin for ElasticSearch
==================================

The RabbitMQ River plugin allows index bulk format messages into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-rabbitmq/1.2.0`.

    --------------------------------------------------------
    | RabbitMQ Plugin | ElasticSearch    | RabbitMQ Client |
    --------------------------------------------------------
    | master          | 0.19 -> master   | 2.8.1           |
    --------------------------------------------------------
    | 1.2.0           | 0.19 -> master   | 2.8.1           |
    --------------------------------------------------------
    | 1.1.0           | 0.19 -> master   | 2.7.0           |
    --------------------------------------------------------
    | 1.0.0           | 0.18             | 2.7.0           |
    --------------------------------------------------------

RabbitMQ River allows to automatically index a [RabbitMQ](http://www.rabbitmq.com/) queue. The format of the messages follows the bulk api format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    

Creating the rabbitmq river is as simple as (all configuration parameters are provided, with default values):

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "rabbitmq",
	    "rabbitmq" : {
	        "host" : "localhost", 
	        "port" : 5672,
	        "user" : "guest",
	        "pass" : "guest",
	        "vhost" : "/",
	        "queue" : "elasticsearch",
	        "exchange" : "elasticsearch",
	        "routing_key" : "elasticsearch",
	        "exchange_type" : "direct",
	        "exchange_durable" : true,
	        "queue_durable" : true,
	        "queue_auto_delete" : false
	    },
	    "index" : {
	        "bulk_size" : 100,
	        "bulk_timeout" : "10ms",
	        "ordered" : false
	    }
	}'

The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed. It can also be used as a simple way to throttle indexing.
