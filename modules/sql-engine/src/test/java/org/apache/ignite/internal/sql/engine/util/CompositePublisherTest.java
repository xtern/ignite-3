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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CompositePublisherTest {
    static class TestPublisher<T> implements Publisher<T> {
        private final T[] data;

        TestPublisher(T[] data) {
            this.data = data;
        }

        class TestSubscription implements Subscription {
            AtomicInteger idx = new AtomicInteger(0);

            @Override
            public void request(long n) {
                System.out.println(">xxx> request " + n);

                CompletableFuture.supplyAsync(() -> {
                    int startIdx = idx.getAndAdd((int)n);
                    int endIdx = Math.min(startIdx + (int)n, data.length);

                    System.out.println(">xxx> push " + Arrays.toString(Arrays.copyOfRange(data, startIdx, endIdx)));

                    for (int n0 = startIdx; n0 < endIdx; n0++) {
                        subscriber.onNext(data[n0]);
                    }

                    if (endIdx >= data.length) {
                        subscriber.onComplete();
                    }

                    return n;
                });
            }

            @Override
            public void cancel() {
                subscriber.onError(new RuntimeException("cancelled"));
            }
        }

        private Subscriber<? super T> subscriber;

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;

            subscriber.onSubscribe(new TestSubscription());
        }
    }

    @Test
    public void testPublisher() throws InterruptedException {
        int dataCnt = 20;
        int requestCnt = 10;
        int threadCnt = 3;
        int totalCnt = threadCnt * dataCnt;
        Integer[][] data = new Integer[threadCnt][dataCnt];
        int[] expData = new int[totalCnt];

        int k = 0;

        for (int i = 0; i < threadCnt; i++) {
            for (int j = 0; j < dataCnt; j++) {
                data[i][j] = ThreadLocalRandom.current().nextInt(totalCnt);

                expData[k++] = data[i][j];
            }

            Arrays.sort(data[i]);
        }

        Arrays.sort(expData);

//        ArrayList<Integer> res = new ArrayList<>();

//        int[] res = new int[threadCnt * dataCnt];
        LinkedBlockingQueue<Integer> res = new LinkedBlockingQueue<>();

        CompositePublisher<Integer> publisher = new CompositePublisher<>(Comparator.comparingInt(v -> v));

        for (int i = 0; i < threadCnt; i++) {
            publisher.add(new TestPublisher<>(data[i]));
        }

        CountDownLatch finishLatch = new CountDownLatch(1);

        publisher.subscribe(new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(requestCnt);
                }

                @Override
                public void onNext(Integer item) {
                    System.out.println(">xxx> " + item);

                    res.add(item);
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onComplete() {
                    System.out.println(">xxx> complete");

                    finishLatch.countDown();
                }
        });

        if (!finishLatch.await(10, TimeUnit.SECONDS))
            fail("Execution timeout");

        int[] resArr = new int[requestCnt];

        k = 0;

        for (Integer n : res) {
            if (resArr.length == k)
                break;

            resArr[k++] = n;
        }

        Assertions.assertArrayEquals(Arrays.copyOf(expData, requestCnt), resArr, "\n" + Arrays.toString(expData) + "\n" + Arrays.toString(resArr) + "\n");

//        if (true)
//            return;
//
//        Thread[] threads = new Thread[threadCnt];
//        Queue<Object> resQueue = new LinkedBlockingQueue<>();
//        MegaAcceptor<Object, Integer> acceptor = new MegaAcceptor<>(threadCnt, v -> {
////            System.out.println(">xxx> submit " + v);
//
//            resQueue.add(v);
//        }, Comparator.comparingInt(v -> v), (t) -> (int)t);
//
//        CyclicBarrier startBarrier = new CyclicBarrier(threadCnt);
//
//        for (int n = 0; n < threadCnt; n++) {
//            int[] arrCp = Arrays.copyOfRange(data, n * dataCnt, (n + 1) * dataCnt);
//
//            Arrays.sort(arrCp);
//
//            threads[n] = new Thread(new TestDataStreamer(startBarrier, n, arrCp, acceptor));
//        }
//
//        for (int n = 0; n < threadCnt; n++)
//            threads[n].start();
//
//        for (int n = 0; n < threadCnt; n++)
//            threads[n].join();
//
//        Arrays.sort(data);
//
//        int[] actData = new int[data.length];
//        int cnt = 0;
//        for (Object obj : resQueue) {
//            actData[cnt++] = (int)obj;
//        }

//        List<Integer> expList = Arrays.stream(data)
//                .boxed()
//                .collect(Collectors.toList());

//        Assertions.assertArrayEquals(data, actData, Arrays.toString(data) + "\n" + Arrays.toString(actData));
    }

    static class MegaAcceptor<T, R> {
        private final T[] recentRows;
        private final Consumer<R> finalConsumer;
        private final ReentrantLock lock = new ReentrantLock();
        private final Comparator<R> cmp;
        private final Set<Integer> finished = new HashSet<>();
        private final Function<T, R> conv;

        private int minIdx = -1;

        MegaAcceptor(int threadCnt, Consumer<R> finalConsumer, Comparator<R> cmp, Function<T, R> conv) {
            this.recentRows = (T[])new Object[threadCnt];
            this.finalConsumer = finalConsumer;
            this.cmp = cmp;
            this.conv = conv;
        }

        private T minValue(int idx) throws InterruptedException {
            int minIdx0 = 0;

            // If the only one left.
            if (finished.size() == recentRows.length - 1) {
                for (int n = 0; n < recentRows.length; n++) {
                    if (!finished.contains(n)) {
                        minIdx = n;

                        if (minIdx != idx)
                            return null;

                        T minVal = recentRows[minIdx];

                        recentRows[minIdx] = null;

                        return minVal;
                    }
                }
            }

            for (int n = 0; n < recentRows.length; n++) {
                T obj = recentRows[n];

                if (obj == null) {
                    if (finished.contains(n)) {
                        if (minIdx0 == n)
                            minIdx0 = n + 1;

                        continue;
                    }

                    minIdx = -1;

                    return null;
                }

//                T val = (T)obj;

                if (cmp.compare(conv.apply(recentRows[minIdx0]), conv.apply(obj)) > 0)
                    minIdx0 = n;
            }

            minIdx = minIdx0;

            if (minIdx0 != idx) {
                notifyAll();

                return null;
            }

            T minVal = recentRows[minIdx0];

            recentRows[minIdx] = null;

            return minVal;
        }

        public synchronized void accept(T o, int idx) {
//            System.out.println(">xxx> " + Arrays.toString(recentRows) + " v=" + o + " idx = " + idx + ", minIdx = " + minIdx);
//            lock.lock();
//
            try {
                while (recentRows[idx] != null && idx != minIdx) {
//                    System.out.println(">xxx> sleep on " + o);

                    wait();
                }

//                System.out.println(">xxx> wake-up " + o);

                if (o == null) {
                    finished.add(idx);

                    assert minIdx == idx;

                    if (recentRows[idx] != null) {
                        T v = recentRows[idx];

                        recentRows[idx] = null;

                        finalConsumer.accept(conv.apply(v));
                    }

                    T v = minValue(idx);

                    notifyAll();

                    assert minIdx != idx;

                    return;
                }

                if (minIdx == idx && recentRows[idx] != null)
                    finalConsumer.accept(conv.apply(recentRows[idx]));

                if (minIdx == -1)
                    minIdx = idx;

                recentRows[idx] = o;

                T v = minValue(idx);

                if (v != null)
                    finalConsumer.accept(conv.apply(v));
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
//                lock.unlock();
            }

        }
    }

    static class TestDataStreamer implements Runnable {
        private final int[] data;
        private final int idx;
        private final MegaAcceptor<Object, Integer> consumer;
        private final CyclicBarrier startBarrier;

        public TestDataStreamer(CyclicBarrier startBarrier, int idx, int[] data, MegaAcceptor<Object, Integer> consumer) {
            this.idx = idx;
            this.data = data;
            this.consumer = consumer;
            this.startBarrier = startBarrier;
        }

        @Override
        public void run() {
            try {
                startBarrier.await();

                for (int i = 0; i < data.length; i++)
                    consumer.accept(data[i], idx);

//                System.out.println(">xxx> finished " + idx);

                consumer.accept(null, idx);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testBlockingPublisher() throws InterruptedException {
        int dataCnt = 1_000;
        int threadCnt = 8;
        int[] data = new int[dataCnt * threadCnt];


        for (int i = 0; i < dataCnt * threadCnt; i++)
            data[i] = ThreadLocalRandom.current().nextInt();

        Thread[] threads = new Thread[threadCnt];
        Queue<Object> resQueue = new LinkedBlockingQueue<>();
        MegaAcceptor<Object, Integer> acceptor = new MegaAcceptor<>(threadCnt, v -> {
//            System.out.println(">xxx> submit " + v);

            resQueue.add(v);
        }, Comparator.comparingInt(v -> v), (t) -> (int)t);

        CyclicBarrier startBarrier = new CyclicBarrier(threadCnt);

        for (int n = 0; n < threadCnt; n++) {
            int[] arrCp = Arrays.copyOfRange(data, n * dataCnt, (n + 1) * dataCnt);

            Arrays.sort(arrCp);

            threads[n] = new Thread(new TestDataStreamer(startBarrier, n, arrCp, acceptor));
        }

        for (int n = 0; n < threadCnt; n++)
            threads[n].start();

        for (int n = 0; n < threadCnt; n++)
            threads[n].join();

        Arrays.sort(data);

        int[] actData = new int[data.length];
        int cnt = 0;
        for (Object obj : resQueue) {
            actData[cnt++] = (int)obj;
        }

//        List<Integer> expList = Arrays.stream(data)
//                .boxed()
//                .collect(Collectors.toList());

        Assertions.assertArrayEquals(data, actData, Arrays.toString(data) + "\n" + Arrays.toString(actData));
    }
}
