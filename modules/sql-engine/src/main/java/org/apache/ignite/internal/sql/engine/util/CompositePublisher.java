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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CompositePublisher<T> implements Flow.Publisher<T> {
    List<Publisher<T>> publishers = new ArrayList<>();

    CompositeSubscription<T> compSubscription;

    AtomicBoolean subscribed = new AtomicBoolean();

    private final Comparator<T> comp;

    public CompositePublisher(Comparator<T> comp) {
        this.comp = comp;

        compSubscription = new CompositeSubscription<>(comp);
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
            publishers.get(i).subscribe(new MagicSubscriber<>(subscriber, i, compSubscription));
        }

        subscriber.onSubscribe(compSubscription);
    }

    private static class MagicSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> delegate;

        private final int idx;

        private final CompositeSubscription<T> compSubscription;

        private List<T> inBuf = new ArrayList<>();

        // todo
        private Subscription subscription;

        MagicSubscriber(Subscriber<T> delegate, int idx, CompositeSubscription<T> compSubscription) {
            assert delegate != null;

            this.delegate = delegate;
            this.idx = idx;
            this.compSubscription = compSubscription;
        }

        private T lastItem() {
            if (!inBuf.isEmpty())
                return inBuf.get(inBuf.size() - 1);

            return null;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            compSubscription.add(this.subscription = subscription, this);
        }

        @Override
        public void onNext(T item) {
            // todo MAGIC
            delegate.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            // todo sync properly
            if (complete()) {
                delegate.onError(throwable);
            }
        }

        @Override
        public void onComplete() {
            // last submitter will choose what to do next
            compSubscription.onRequestCompleted();

//            System.out.println(">xxx> complete " + idx);
            // todo sync properly
//            if (complete()) {
////                System.out.println(">xxx> completed");
//                delegate.onComplete();
//            }
        }

        boolean complete() {
            return compSubscription.remove(subscription) && compSubscription.subscriptions().isEmpty();
        }
    }

    private static class CompositeSubscription<T> implements Subscription {

        private final Comparator<T> comp;

        private List<Subscription> subscriptions = new ArrayList<>();

        private final List<MagicSubscriber<T>> subscribers = new ArrayList<>();

        int minIdx = 0;

        private final AtomicInteger requestCompleted = new AtomicInteger();

        private CompositeSubscription(Comparator<T> comp) {
            this.comp = comp;
        }

        public void onRequestCompleted() {
            // Internal buffers has been filled.
            if (requestCompleted.incrementAndGet() == subscriptions.size()) {
                requestCompleted.set(0);


            }
        }

        private int selectMinIdx() {
            T minItem = null;

            for (MagicSubscriber<T> subcriber : subscribers) {
                subcriber.lastItem();

                int size = subcriber.inBuf.size()
            }
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
            requestCompleted.set(0);

            long requestCnt = Math.max(1, n / subscriptions.size());

            for (Subscription subscription : subscriptions) {
                subscription.request(requestCnt);
            }
        }

        @Override
        public void cancel() {
            // todo sync
            for (Subscription subscription : subscriptions) {
                subscription.cancel();
            }
        }
    }
}
