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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

/**
 * This Nacker state machine helps to deal with delivery tags we need to ACK or NACK at
 * some point.
 *
 * It keeps the current delivery known tag in `currentTag` field and if we have any error
 * with a delivery tag, it's also stored in `failedTag`.
 *
 * When the current tag changes because we are basically reading another message from RabbitMQ,
 * we can ACK or NACK the previous stored delivery tag depending on rules:
 *
 * <ul>
 *     <li>If `rabbitNackErrors` is set to false, we systematically ack the stored tag</li>
 *     <li>If `rabbitNackErrors` is set to true and failedTag has not been set (-1L),
 *     we ack the stored tag</li>
 *     <li>If `rabbitNackErrors` is set to true and failedTag has been set,
 *     we nack the stored tag</li>
 * </ul>
 */
class Nacker {

    private final ESLogger logger = ESLoggerFactory.getLogger(Nacker.class.getName());
    private final boolean rabbitNackErrors;

    enum Status {
        ACK, NACK, NONE
    }

    public Nacker(boolean rabbitNackErrors) {
        this.rabbitNackErrors = rabbitNackErrors;
    }

    private long currentTag = -1L;
    private long failedTag = -1L;


    public long getCurrentTag() {
        return currentTag;
    }

    public long getFailedTag() {
        return failedTag;
    }

    public Tuple<Status, Long> addDeliveryTag(long tag) {
        Tuple<Status, Long> state = new Tuple<Status, Long>(Status.NONE, -1L);
        // We check if the tag is already the current one.
        if (currentTag != tag) {
            if (currentTag != -1L) {
                // Ack if needed!
                // First we check if this tag is also an error tag.
                // If we did not set nack_errors, we ack even if there is an error
                if (currentTag != failedTag || !rabbitNackErrors) {
                    // We can ack it
                    state = new Tuple<Status, Long>(Status.ACK, currentTag);
                } else {
                    // We need to nack
                    state = new Tuple<Status, Long>(Status.NACK, currentTag);
                }
            }
            // Reset currentTag and failedTag
            currentTag = tag;
            failedTag = -1L;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("added delivery tag [{}]: [{}]", tag, state);
        }
        return state;
    }

    public Tuple<Status, Long> addFailedTag(long tag) {
        Tuple<Status, Long> state =new Tuple<Status, Long>(Status.NONE, -1L);
        // We check if the tag is already the current one.
        if (currentTag != tag && currentTag != -1L) {
            if (failedTag != -1L) {
                // If needed, we will nack it
                if (rabbitNackErrors) {
                    state = new Tuple<Status, Long>(Status.NACK, failedTag);
                } else {
                    // We need to ack previous tag
                    state = new Tuple<Status, Long>(Status.ACK, failedTag);
                }
            } else {
                // We need to ack previous tag
                state = new Tuple<Status, Long>(Status.ACK, currentTag);
            }
        }

        currentTag = tag;
        failedTag = tag;
        if (logger.isTraceEnabled()) {
            logger.trace("added failed tag [{}]: [{}]", tag, state);
        }
        return state;
    }

    public Tuple<Status, Long> forceAck() {
        Tuple<Status, Long> state = new Tuple<Status, Long>(Status.NONE, -1L);
        if (currentTag != -1L) {
            if (currentTag != failedTag || !rabbitNackErrors) {
                // We can ack it
                state = new Tuple<Status, Long>(Status.ACK, currentTag);
            } else {
                // We need to nack
                state = new Tuple<Status, Long>(Status.NACK, currentTag);
            }
            currentTag = -1L;
            failedTag = -1L;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("forcing ack: [{}]", state);
        }
        return state;
    }
}
