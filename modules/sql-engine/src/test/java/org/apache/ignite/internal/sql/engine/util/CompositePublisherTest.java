/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

public class CompositePublisherTest {
    static class MegaAcceptor {
        private final Object[] recentRows;

        private final Consumer<Object> finalConsumer;

        MegaAcceptor(Object[] recentRows, Consumer<Object> finalConsumer) {
            this.recentRows = recentRows;
            this.finalConsumer = finalConsumer;
        }

//        @Override
        public void accept(Object o, int idx) {
            finalConsumer.accept(o);
        }
    }

    static class DataStreamer implements Runnable {

        private final int[] data;

        private final MegaAcceptor consumer;
        private final int idx;

        public DataStreamer(int idx, int[] data, MegaAcceptor consumer) {
            this.idx = idx;
            this.data = data;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            for (int i = 0; i < data.length; i++)
                consumer.accept(data[i], idx);
        }
    }

    @Test
    public void testPublisher() throws InterruptedException {
        int dataCnt = 10;
        int threadCnt = 3;
        int[] data = new int[dataCnt * threadCnt];


        for (int i = 0; i < dataCnt * threadCnt; i++)
            data[i] = ThreadLocalRandom.current().nextInt();

        Thread[] threads = new Thread[threadCnt];

//        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        List<Object> list = new ArrayList<>();

        MegaAcceptor acceptor = new MegaAcceptor(null, list::add);

        for (int n = 0; n < threadCnt; n++) {
            threads[n] = new Thread(new DataStreamer(n, Arrays.copyOfRange(data, n * dataCnt, (n + 1) * dataCnt), acceptor));
        }

        for (int n = 0; n < threadCnt; n++) {
            threads[n].start();
        }

        for (int n = 0; n < threadCnt; n++) {
            threads[n].join();
        }

        Arrays.sort(data);

        int[] actData = new int[dataCnt * threadCnt];

        int n = 0;

        for (Object obj : list) {
            actData[n++] = (int)obj;
        }

//        List<Object> list = new ArrayList<>(queue);

        Assertions.assertArrayEquals(data, actData);
    }
}
