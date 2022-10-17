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
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.schema.BinaryTuple;

public class CompositePublisher implements Flow.Publisher<BinaryTuple> {
    List<Publisher<BinaryTuple>> publishers = new ArrayList<>();

    CompositeSubscription compSubscription = new CompositeSubscription();

    AtomicBoolean subscribed = new AtomicBoolean();

    public void add(Publisher<BinaryTuple> publisher) {
        publishers.add(publisher);
    }

    @Override
    public void subscribe(Subscriber<? super BinaryTuple> subscriber) {
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

        private final CompositeSubscription compSubscription;

        // todo
        private Subscription subscription;

        MagicSubscriber(Subscriber<T> delegate, int idx, CompositeSubscription compSubscription) {
            assert delegate != null;

            this.delegate = delegate;
            this.idx = idx;
            this.compSubscription = compSubscription;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            compSubscription.add(this.subscription = subscription);
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
            // todo sync properly
            if (complete()) {
                System.out.println(">xxx> completed");
                delegate.onComplete();
            }
        }

        boolean complete() {
            return compSubscription.remove(subscription) && compSubscription.subscriptions().isEmpty();
        }
    }

    private static class CompositeSubscription implements Subscription {

        private List<Subscription> subscriptions = new ArrayList<>();

        public List<Subscription> subscriptions() {
            return subscriptions;
        }

        public void add(Subscription subscription) {
            subscriptions.add(subscription);
        }

        // todo sync
        public boolean remove(Subscription subscription) {
            return subscriptions.remove(subscription);
        }

        @Override
        public void request(long n) {
            // todo sync
            for (Subscription subscription : subscriptions) {
                subscription.request(n);
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
