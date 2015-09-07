**Important**: This project has been stopped since elasticsearch 2.0.

----

RabbitMQ River Plugin for Elasticsearch
==================================

The RabbitMQ River plugin allows index [bulk format messages](http://www.elasticsearch.org/guide/reference/api/bulk/) into elasticsearch.
RabbitMQ River allows to automatically index a [RabbitMQ](http://www.rabbitmq.com/) queue. The format of the messages follows the bulk api format:

```javascript
{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
{ "tweet" : { "text" : "this is a tweet" } }
{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
{ "tweet" : { "text" : "another tweet" } }
```

**Rivers are [deprecated](https://www.elastic.co/blog/deprecating_rivers) and will be removed in the future.**
Have a look at [logstash rabbitmq input](http://www.elastic.co/guide/en/logstash/current/plugins-inputs-rabbitmq.html).

In order to install the plugin, run: 

```sh
bin/plugin install elasticsearch/elasticsearch-river-rabbitmq/2.6.0
```

You need to install a version matching your Elasticsearch version:

|       Elasticsearch    | RabbitMQ River    |                                                             Docs                                                                   |
|------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------|
|    master              | Build from source | See below                                                                                                                          |
|    es-1.x              | Build from source  | [2.7.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/es-1.x/#version-270-snapshot-for-elasticsearch-1x)|
|    es-1.6              |     2.6.0         | [2.6.0](https://github.com/elastic/elasticsearch-river-rabbitmq/tree/v2.6.0/#version-260-for-elasticsearch-16)                  |
|    es-1.5              |     2.5.0         | [2.5.0](https://github.com/elastic/elasticsearch-river-rabbitmq/tree/v2.5.0/#version-250-for-elasticsearch-15)                  |
|    es-1.4              |     2.4.1         | [2.4.1](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/v2.4.1/#version-241-for-elasticsearch-14)                  |
|    es-1.3              |     2.3.0         | [2.3.0](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/v2.3.0/#version-230-for-elasticsearch-13)                  |
|    es-1.2              |     2.2.0         | [2.2.0](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/v2.2.0/#version-220-for-elasticsearch-12)                  |
|    es-1.1              |     2.0.0         | [2.0.0](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/v2.0.0/#rabbitmq-river-plugin-for-elasticsearch)  |
|    es-1.0              |     2.0.0         | [2.0.0](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/v2.0.0/#rabbitmq-river-plugin-for-elasticsearch)  |
|    es-0.90             |     1.6.0         | [1.6.0](https://github.com/elasticsearch/elasticsearch-river-rabbitmq/tree/v1.6.0/#rabbitmq-river-plugin-for-elasticsearch)  |

To build a `SNAPSHOT` version, you need to build it with Maven:

```bash
mvn clean install
plugin --install river-rabbitmq \ 
       --url file:target/releases/elasticsearch-river-rabbitmq-X.X.X-SNAPSHOT.zip
```

Create river
------------

Creating the rabbitmq river is as simple as (all configuration parameters are provided, with default values):

```sh
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
        "exchange_declare" : true,
        "exchange_type" : "direct",
        "exchange_durable" : true,
        "queue_declare" : true,
        "queue_bind" : true,
        "queue_durable" : true,
        "queue_auto_delete" : false,
        "heartbeat" : "30m",
        "qos_prefetch_size" : 0,
        "qos_prefetch_count" : 10,
        "nack_errors" : true
    },
    "index" : {
        "bulk_size" : 100,
        "bulk_timeout" : "10ms",
        "ordered" : false,
        "replication" : "default"
    }
}'
```

You can disable exchange or queue declaration by setting `exchange_declare` or `queue_declare` to `false`
(`true` by default).
You can disable queue binding by setting `queue_bind` to `false` (`true` by default).

Addresses(host-port pairs) also available. it is useful to taking advantage rabbitmq HA(active/active) without any rabbitmq load balancer.
(http://www.rabbitmq.com/ha.html)

```javascript
    ...
    "rabbitmq" : {
        "addresses" : [
            {
                "host" : "rabbitmq-host1",
                "port" : 5672
            },
            {
                "host" : "rabbitmq-host2",
                "port" : 5672
            }
        ],
        "user" : "guest",
        "pass" : "guest",
        "vhost" : "/",
        ...
    }
    ...
```

The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the
messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the
same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed.
It can also be used as a simple way to throttle indexing.

You can set `heartbeat` option to define heartbeat to RabbitMQ river even if no more messages are intended to be consumed
(default to `30m`).

Replication mode is set to node default value. You can change it by forcing `replication` to `async` or `sync`.

By default, when exception happens while executing bulk, failing messages are marked as rejected.
You can ignore errors and ack messages in any case setting `nack_errors` to `false`.

Setting `qos_prefetch_size` will define maximum amount of content (measured in octets) that the server will deliver 
(0 if unlimited - default).

Setting `qos_prefetch_count` will define maximum number of messages that the server will deliver (0 if unlimited). 
Default to `bulk_size*2`.

Scripting
---------

RabbitMQ river can call scripts to modify or filter messages.

### Full bulk scripting

To enable bulk scripting use the following configuration options:

```sh
curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
    "type" : "rabbitmq",
    "rabbitmq" : {
        ...
    },
    "index" : {
        ...
    },
    "bulk_script_filter" : {
        "script" : "myscript",
        "script_lang" : "native",
        "script_params" : {
            "param1" : "val1",
            "param2" : "val2"
            ...
        }
    }
}'
```

* `script` is optional and is the name of the registered script in `elasticsearch.yml`. Basically, add the following
property: `script.native.myscript.type: sample.MyNativeScriptFactory` and provide this class to elasticsearch
classloader.
* `script_lang` is by default `native`.
* `script_params` are optional configuration arguments for the script.

The script will receive a variable called `body` which contains a String representation of RabbitMQ's message body.
That `body` can be modified by the script, and it must return the new body as a String as well.
If the returned body is null, that message will be skipped from the indexing flow.

For more information see [Scripting module](http://www.elasticsearch.org/guide/reference/modules/scripting/)

### Doc per doc scripting

You may also want to apply scripts document per document. It will only works for index or create operations.

To enable scripting use the following configuration options:

```sh
curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
    "type" : "rabbitmq",
    "rabbitmq" : {
        ...
    },
    "index" : {
        ...
    },
    "script_filter" : {
        "script" : "ctx.type1.field1 += param1",
        "script_lang" : "mvel",
        "script_params" : {
          "param1" : 1
        }
    }
}'
```

* `script` is your javascript code if you use `mvel` scripts.
* `script_lang` is by default `mvel`.
* `script_params` are optional configuration arguments for the script.

The script will receive a variable called `ctx` which contains a String representation of the current document
meant to be indexed or created.

For more information see [Scripting module](http://www.elasticsearch.org/guide/reference/modules/scripting/)

Tests
=====

Integrations tests in this plugin require working RabbitMQ service and therefore disabled by default. 
You need to launch locally `rabbitmq-server` before starting integration tests.

To run test:

```sh
mvn clean test -Dtests.rabbitmq=true 
```


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
