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
import org.elasticsearch.test.ElasticsearchTestCase;

import static org.hamcrest.CoreMatchers.is;

public class NackAckTest extends ElasticsearchTestCase {


    public void testScenarioWithNackOff() {
        simulateScenario(false);
    }

    public void testScenarioWithNackOn() {
        simulateScenario(true);
    }

    public void simulateScenario(boolean nack) {

        Nacker nacker = new Nacker(nack);

        // We send 2 OK tags "1"
        simulateTag(nacker, nack, true, 1, 1L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));
        simulateTag(nacker, nack, true, 1, 1L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We send 2 OK tags "2".
        // "1" should be ack'ed.
        simulateTag(nacker, nack, true, 2, 2L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.ACK, 1L));
        simulateTag(nacker, nack, true, 2, 2L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We send 1 KO tags "2".
        // Nothing should happen.
        simulateTag(nacker, nack, false, 2, 2L, 2L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We send 2 OK tags "2".
        // Nothing should happen.
        simulateTag(nacker, nack, true, 2, 2L, 2L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));
        simulateTag(nacker, nack, true, 2, 2L, 2L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We send 2 OK tags "3".
        // "2" should be nack'ed.
        simulateTag(nacker, nack, true, 3, 3L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NACK, 2L));
        simulateTag(nacker, nack, true, 3, 3L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We send 1 KO tag "4"
        // "3" should be ack'ed
        simulateTag(nacker, nack, false, 4, 4L, 4L, new Tuple<Nacker.Status, Long>(Nacker.Status.ACK, 3L));

        // We send 1 KO tag "5"
        // "4" should be nack'ed
        simulateTag(nacker, nack, false, 5, 5L, 5L, new Tuple<Nacker.Status, Long>(Nacker.Status.NACK, 4L));

        // We send 1 OK tag "6"
        // "5" should be nack'ed
        simulateTag(nacker, nack, true, 6, 6L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NACK, 5L));

        // We force a final ack
        // "6" should be ack'ed
        simulateForceAck(nacker, nack, -1L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.ACK, 6L));

        // We force again.
        // Nothing should happen.
        simulateForceAck(nacker, nack, -1L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We send 1 KO tag "7"
        // Nothing should happen.
        simulateTag(nacker, nack, false, 7, 7L, 7L, new Tuple<Nacker.Status, Long>(Nacker.Status.NONE, -1L));

        // We force a final ack
        // "7" should be nack'ed
        simulateForceAck(nacker, nack, -1L, -1L, new Tuple<Nacker.Status, Long>(Nacker.Status.NACK, 7L));

    }

    private void simulateForceAck(Nacker nacker, boolean nack,
                                  long expectedCurrent, long expectedFailed,
                                  Tuple<Nacker.Status, Long> expectedStatus) {
        logger.info(" -> force ack'ing");
        logger.debug("   -> Before nacker is current [{}], failed [{}]", nacker.getCurrentTag(), nacker.getFailedTag());
        Tuple<Nacker.Status, Long> statusLongTuple = nacker.forceAck();
        logger.debug("   -> We did [{}] for tag [{}]", statusLongTuple.v1(), statusLongTuple.v2());
        logger.debug("   -> After nacker is current [{}], failed [{}]", nacker.getCurrentTag(), nacker.getFailedTag());
        assertThat(nacker.getCurrentTag(), is(expectedCurrent));
        assertThat(nacker.getFailedTag(), is(expectedFailed));

        if (nack) {
            assertThat(statusLongTuple, is(expectedStatus));
        } else {
            if (expectedStatus.v1().equals(Nacker.Status.NACK)) {
                expectedStatus = new Tuple<Nacker.Status, Long>(Nacker.Status.ACK, expectedStatus.v2());
            }
            assertThat(statusLongTuple, is(expectedStatus));
        }
    }

    private void simulateTag(Nacker nacker, boolean nack, boolean ok, long tag,
                                     long expectedCurrent, long expectedFailed,
                                     Tuple<Nacker.Status, Long> expectedStatus) {
        logger.info(" -> sending [{}] tag: [{}].", ok ? "OK" : "KO", tag);
        logger.debug("   -> Before nacker is current [{}], failed [{}]", nacker.getCurrentTag(), nacker.getFailedTag());
        Tuple<Nacker.Status, Long> statusLongTuple = ok ? nacker.addDeliveryTag(tag) : nacker.addFailedTag(tag);
        logger.debug("   -> We did [{}] for tag [{}]", statusLongTuple.v1(), statusLongTuple.v2());
        logger.debug("   -> After nacker is current [{}], failed [{}]", nacker.getCurrentTag(), nacker.getFailedTag());
        assertThat(nacker.getCurrentTag(), is(expectedCurrent));
        assertThat(nacker.getFailedTag(), is(expectedFailed));

        if (nack) {
            assertThat(statusLongTuple, is(expectedStatus));
        } else {
            if (expectedStatus.v1().equals(Nacker.Status.NACK)) {
                expectedStatus = new Tuple<Nacker.Status, Long>(Nacker.Status.ACK, expectedStatus.v2());
            }
            assertThat(statusLongTuple, is(expectedStatus));
        }
    }
}
