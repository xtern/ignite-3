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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CompositePublisher<T> implements Flow.Publisher<T> {
    List<Publisher<T>> publishers = new ArrayList<>();

    CompositeSubscription<T> compSubscription;

    AtomicBoolean subscribed = new AtomicBoolean();

    private final Comparator<T> comp;

    private final PriorityBlockingQueue<T> queue;

    public CompositePublisher(Comparator<T> comp) {
        this.comp = comp;

        this.queue = new PriorityBlockingQueue<>(1, comp);

        compSubscription = new CompositeSubscription<>(comp, queue);
    }

    public void add(Publisher<T> publisher) {
        publishers.add(publisher);
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        // todo sync
        if (!subscribed.compareAndSet(false, true)) {
            throw new IllegalStateException("Support only one subscriber");
        }

        for (int i = 0; i < publishers.size(); i++) {
            MagicSubscriber<? super T> subs = new MagicSubscriber<>((Subscriber<T>) subscriber, i, compSubscription, queue);

            publishers.get(i).subscribe(subs);
        }

        subscriber.onSubscribe(compSubscription);
    }

    private static class MagicSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> delegate;

        private final int idx;

        private final CompositeSubscription<T> compSubscription;
        private final PriorityBlockingQueue<T> queue;

//        private List<T> inBuf = new ArrayList<>();

        // todo
        private Subscription subscription;

        private volatile T lastItem;

        private final AtomicLong remainingCnt = new AtomicLong();

        private volatile boolean finished;

        MagicSubscriber(Subscriber<T> delegate, int idx, CompositeSubscription<T> compSubscription, PriorityBlockingQueue<T> queue) {
            assert delegate != null;

            this.delegate = delegate;
            this.idx = idx;
            this.compSubscription = compSubscription;
            this.queue = queue;
        }

        private T lastItem() {

            return lastItem;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            compSubscription.add(this.subscription = subscription, this);
        }

        @Override
        public void onNext(T item) {
            // todo optimize
            lastItem = item;

            queue.add(item);

            if (remainingCnt.decrementAndGet() <= 0) {
                if (remainingCnt.get() != 0) {
                    System.err.println("!!!!remaining failed");
                }

                compSubscription.onRequestCompleted();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();

            // todo sync properly
            if (complete()) {
                delegate.onError(throwable);
            }
        }

        public void onDataRequested(long n) {
            remainingCnt.set(n);
        }

        @Override
        public void onComplete() {
            finished = true;

            compSubscription.subscriptionFinished(idx);
            // last submitter will choose what to do next
//            compSubscription.onRequestCompleted();

//            System.out.println(">xxx> complete " + idx);
            // todo sync properly
//            if (complete()) {
////                System.out.println(">xxx> completed");
//                delegate.onComplete();
//            }
        }

        public long pushQueue(long remain, Comparator<T> comp) {
            boolean done = false;
            int pushedCnt = 0;
            T r = null;

            while (remain > 0 && (r = queue.peek()) != null) {
                boolean same = comp.compare(lastItem, r) == 0;

                if (!done && same)
                    done = true;

                if (!done || same) {
                    delegate.onNext(queue.poll());

                    --remain;
                }

                if (done && !same)
                    break;
            }

            if (remain == 0)
                delegate.onComplete();

            return remain;
        }

        boolean complete() {
            return compSubscription.remove(subscription) && compSubscription.subscriptions().isEmpty();
        }
    }

    private static class CompositeSubscription<T> implements Subscription {

        private final Comparator<T> comp;

        private final PriorityBlockingQueue<T> queue;

        private List<Subscription> subscriptions = new ArrayList<>();

        private final List<MagicSubscriber<T>> subscribers = new ArrayList<>();

//        int minIdx = 0;

        volatile long remain = 0;

        volatile long requested = 0;

        private final AtomicInteger requestCompleted = new AtomicInteger();

        private CompositeSubscription(Comparator<T> comp, PriorityBlockingQueue<T> queue) {
            this.comp = comp;
            this.queue = queue;
        }

        public void onRequestCompleted() {
            // Internal buffers has been filled.
            if (requestCompleted.incrementAndGet() == subscriptions.size()) {
//                requestCompleted.set(0);

                List<Integer> minIdxs = selectMinIdx();

                MagicSubscriber<T> subscr = subscribers.get(minIdxs.get(0));

                assert subscr != null && !subscr.finished;

                // todo
                System.out.println(">xxx> pushQueue");
                try {
                    remain = subscr.pushQueue(remain, comp);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                System.out.println(">xxx> pushQueue :: end");

                if (remain > 0) {
                    long dataAmount = Math.max(1, requested / subscriptions.size());

                    for (Integer idx : minIdxs) {
                        requestCompleted.decrementAndGet();

                        subscribers.get(idx).onDataRequested(dataAmount);
                    }

                    for (Integer idx : minIdxs) {
                        System.out.println(">xxx> idx =" + idx + " requested=" + dataAmount);

                        subscriptions.get(idx).request(dataAmount);
                    }
                } else {
                    System.out.println(">xxx> remin is zero");
                }
            } else {
                System.out.println(">xxx> waiting " + (subscriptions.size() - requestCompleted.get()));
            }
        }

        private List<Integer> selectMinIdx() {
            T minItem = null;
            List<Integer> minIdxs = new ArrayList<>();

            for (int i = 0; i < subscribers.size(); i++) {
                MagicSubscriber<T> subcriber = subscribers.get(i);

                if (subcriber == null || subcriber.finished)
                    continue;

                T item = subcriber.lastItem();

                int cmpRes = 0;;

                if (minItem == null || (cmpRes = comp.compare(minItem, item)) >= 0) {
                    minItem = item;

                    if (cmpRes != 0)
                        minIdxs.clear();

                    minIdxs.add(i);
                }
            }

            return minIdxs;
        }

        public List<Subscription> subscriptions() {
            return subscriptions;
        }

        public void add(Subscription subscription, MagicSubscriber<T> subscriber) {
            subscriptions.add(subscription);

            subscribers.add(subscriber);
        }

        // todo sync
        public boolean remove(Subscription subscription) {
            return subscriptions.remove(subscription);
        }

        @Override
        public void request(long n) {
            remain = n;
            requested = n;

            requestCompleted.set(0);

            long requestCnt = Math.max(1, n / subscriptions.size());

            for (int i = 0; i < subscriptions.size(); i++) {
                subscribers.get(i).onDataRequested(requestCnt);
                subscriptions.get(i).request(requestCnt);
            }
        }

        @Override
        public void cancel() {
            // todo sync
            for (Subscription subscription : subscriptions) {
                subscription.cancel();
            }
        }

        public void subscriptionFinished(int idx) {
            subscriptions.set(idx, null);
        }
    }
}
