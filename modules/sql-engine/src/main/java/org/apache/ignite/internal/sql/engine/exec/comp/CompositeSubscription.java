package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class CompositeSubscription<T> implements Subscription {

    private final Comparator<T> comp;

    private final PriorityBlockingQueue<T> queue;

    private List<Subscription> subscriptions = new ArrayList<>();

    private final List<SortingSubscriber<T>> subscribers = new ArrayList<>();

//        int minIdx = 0;

    volatile long remain = 0;

    volatile long requested = 0;

    private final AtomicInteger requestCompleted = new AtomicInteger();

    CompositeSubscription(Comparator<T> comp, PriorityBlockingQueue<T> queue) {
        this.comp = comp;
        this.queue = queue;
    }

    public void onRequestCompleted() {
        // Internal buffers has been filled.
        if (requestCompleted.incrementAndGet() == subscriptions.size()) {
//                requestCompleted.set(0);

            List<Integer> minIdxs = selectMinIdx();

            SortingSubscriber<T> subscr = subscribers.get(minIdxs.get(0));

            assert subscr != null && !subscr.finished();

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
            SortingSubscriber<T> subcriber = subscribers.get(i);

            if (subcriber == null || subcriber.finished()) {
                continue;
            }

            T item = subcriber.lastItem();

            int cmpRes = 0;
            ;

            if (minItem == null || (cmpRes = comp.compare(minItem, item)) >= 0) {
                minItem = item;

                if (cmpRes != 0) {
                    minIdxs.clear();
                }

                minIdxs.add(i);
            }
        }

        return minIdxs;
    }

    public List<Subscription> subscriptions() {
        return subscriptions;
    }

    public void add(Subscription subscription, SortingSubscriber<T> subscriber) {
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
