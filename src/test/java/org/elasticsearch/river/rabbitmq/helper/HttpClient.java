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
package org.elasticsearch.river.rabbitmq.helper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class HttpClient {

    private final URL baseUrl;

    public HttpClient(String hostname, Integer port) {
        try {
            baseUrl = new URL("http", hostname, port, "/");
        } catch (MalformedURLException e) {
            throw new ElasticsearchException("", e);
        }
    }

    public HttpClientResponse request(String path) {
        return request("GET", path, null, null);
    }

    public HttpClientResponse request(String method, String path) {
        return request(method, path, null, null);
    }

    public HttpClientResponse request(String method, String path, String payload) {
        return request(method, path, null, payload);
    }

    public HttpClientResponse request(String method, String path, Map<String, String> headers, String payload) {
        URL url;
        try {
            url = new URL(baseUrl, path);
        } catch (MalformedURLException e) {
            throw new ElasticsearchException("Cannot parse " + path, e);
        }

        HttpURLConnection urlConnection;
        try {
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod(method);
            if (headers != null) {
                for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
                    urlConnection.setRequestProperty(headerEntry.getKey(), headerEntry.getValue());
                }
            }

            if (payload != null) {
                urlConnection.setDoOutput(true);
                urlConnection.setRequestProperty("Content-Type", "application/json");
                urlConnection.setRequestProperty("Accept", "application/json");
                OutputStreamWriter osw = new OutputStreamWriter(urlConnection.getOutputStream());
                osw.write(payload);
                osw.flush();
                osw.close();
            }

            urlConnection.connect();
        } catch (IOException e) {
            throw new ElasticsearchException("", e);
        }

        int errorCode = -1;
        Map<String, List<String>> respHeaders = null;
        try {
            errorCode = urlConnection.getResponseCode();
            respHeaders = urlConnection.getHeaderFields();
            InputStream inputStream = urlConnection.getInputStream();
            String body = null;
            try {
                body = Streams.copyToString(new InputStreamReader(inputStream, Charsets.UTF_8));
            } catch (IOException e1) {
                throw new ElasticsearchException("problem reading error stream", e1);
            }
            return new HttpClientResponse(body, errorCode, respHeaders, null);
        } catch (IOException e) {
            InputStream errStream = urlConnection.getErrorStream();
            String body = null;
            if (errStream != null) {
                try {
                    body = Streams.copyToString(new InputStreamReader(errStream, Charsets.UTF_8));
                } catch (IOException e1) {
                    throw new ElasticsearchException("problem reading error stream", e1);
                }
            }
            return new HttpClientResponse(body, errorCode, respHeaders, e);
        } finally {
            urlConnection.disconnect();
        }
    }
}
