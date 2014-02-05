RabbitMQ River Plugin for Elasticsearch
==================================

The RabbitMQ River plugin allows index [bulk format messages](http://www.elasticsearch.org/guide/reference/api/bulk/) into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-rabbitmq/2.0.0.RC1`.

|    RabbitMQ River Plugin   |    elasticsearch    | RabbitMQ Client | Release date |
|----------------------------|---------------------|-----------------|:------------:|
| 2.0.0-SNAPSHOT (master)    | 1.0.0.RC1 -> master |    3.2.2        |              |
| 2.0.0.RC1                  | 1.0.0.RC1 -> master |    3.2.2        |  2014-01-15  |
| 1.7.0-SNAPSHOT (1.x)       | 0.90                |    3.2.2        |              |
| 1.6.0                      | 0.90                |    3.1.1        |  2013-06-05  |
| 1.5.0                      | 0.19 -> 0.20        |    3.1.0        |  2013-05-16  |
| 1.4.0                      | 0.19                |    2.8.4        |  2012-07-31  |
| 1.3.0                      | 0.19                |    2.8.2        |  2012-06-07  |
| 1.2.0                      | 0.19                |    2.8.1        |  2012-04-27  |
| 1.1.0                      | 0.19                |    2.7.0        |  2012-02-07  |
| 1.0.0                      | 0.18                |    2.7.0        |  2011-12-05  |

RabbitMQ River allows to automatically index a [RabbitMQ](http://www.rabbitmq.com/) queue. The format of the messages follows the bulk api format:

```javascript
{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
{ "tweet" : { "text" : "this is a tweet" } }
{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
{ "tweet" : { "text" : "another tweet" } }
```

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
        "qos_prefetch_size" : 0,
        "qos_prefetch_count" : 10,
        "heartbeat" : "30m"
    },
    "index" : {
        "bulk_size" : 100,
        "bulk_timeout" : "10ms",
        "ordered" : false
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
