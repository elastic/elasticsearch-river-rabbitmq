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

package org.elasticsearch.river.rabbitmq.script;

import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.script.AbstractExecutableScript;

import java.io.*;
import java.util.Map;

public class MockScript extends AbstractExecutableScript {

    private final ESLogger logger = ESLoggerFactory.getLogger(MockScript.class.getName());
    private final Map<String, Object> params;

    public MockScript(Map<String, Object> params) {
        super();
        this.params = params;
    }

    @Override
    public void setNextVar(String name, Object value) {
        params.put(name, value);
    }

    @Override
    public Object run() {
        String body = (String) params.get("body");
        BufferedReader reader = new BufferedReader(new StringReader(body));

        CharArrayWriter charArrayWriter = new CharArrayWriter();
        BufferedWriter writer = new BufferedWriter(charArrayWriter);

        try {
            process(reader, writer);
        } catch (IOException e) {
            // TODO: wrap or treat it
            throw new RuntimeException(e);
        }

        String outputBody = charArrayWriter.toString();
        logger.debug("input message: {}", body);
        logger.debug("output message: {}", outputBody);

        return outputBody;
    }

    private void process(BufferedReader reader, BufferedWriter writer) throws IOException {
        JsonFactory factory = new JsonFactory();
        for (String header = reader.readLine(); header != null; header = reader.readLine()) {
            String content = null;
            JsonXContentParser parser = new JsonXContentParser(factory.createParser(header));
            Map<String, Object> headerAsMap = parser.map();

            if (headerAsMap.containsKey("create") ||
                    headerAsMap.containsKey("index") ||
                    headerAsMap.containsKey("update")) {
                // skip "create" operations, header and body
                content = reader.readLine();

                JsonXContentParser contentParser = new JsonXContentParser(factory.createParser(content));
                Map<String, Object> contentAsMap = contentParser.map();

                Object numeric = contentAsMap.get("numeric");
                if (numeric != null) {
                    if (numeric instanceof Integer) {
                        Integer integer = (Integer) numeric;
                        contentAsMap.put("numeric", ++integer);

                        content = XContentFactory.jsonBuilder().map(contentAsMap).string();
                    } else {
                        logger.warn("We don't know what to do with that numeric value: {}", numeric.getClass().getName());
                    }
                }
            } else if (headerAsMap.containsKey("delete")) {
                // No content line
            } else {
                // That's bad. We don't know what to do :(
                logger.warn("We don't know what to do with that line: {}", header);
            }
            writer.write(header);
            writer.newLine();
            if (content != null) {
                writer.write(content);
                writer.newLine();
            }
        }
        writer.flush();
        writer.close();
    }
}
