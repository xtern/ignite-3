package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;

public class CompositeSubscription<T> implements Subscription {

    private final Comparator<T> comp;

    private final PriorityBlockingQueue<T> queue;

    private final List<Subscription> subscriptions = new ArrayList<>();

    private final List<SortingSubscriber<T>> subscribers = new ArrayList<>();

    private final ConcurrentHashSet<Integer> finished = new ConcurrentHashSet<>();

    private final AtomicBoolean completed = new AtomicBoolean();

    volatile long remain = 0;

    volatile long requested = 0;

    private final AtomicInteger requestCompleted = new AtomicInteger();

    CompositeSubscription(Comparator<T> comp, PriorityBlockingQueue<T> queue) {
        this.comp = comp;
        this.queue = queue;
    }

    public int activeSubcribers() {
        return subscriptions.size() - finished.size();
    }

    public void onRequestCompleted() {
        // Internal buffers has been filled.
        if (requestCompleted.incrementAndGet() >= activeSubcribers()) {
            System.out.println(">xxx> reqs=" + requestCompleted.get() + ", subs=" + subscriptions.size() + ", finished=" + finished.size());

            onRequestCompleted0();
        } else {
            System.out.println(">xxx> waiting " + (subscriptions.size() - activeSubcribers()));
        }
    }

    public void onRequestCompleted0() {
        if (activeSubcribers() == 0) {
            if (completed.compareAndSet(false, true)) {
                System.out.println(">xxx> pushqueue ");
                synchronized (this) {
                    subscribers.get(0).pushQueue(remain, null);
                }
            }

            // all work done
            return;
        }

        List<Integer> minIdxs = selectMinIdx();

        SortingSubscriber<T> subscr = subscribers.get(minIdxs.get(0));

        assert subscr != null && !subscr.finished();

        System.out.println(">xxx> pushQueue :: start");

        synchronized (this) {
            remain = subscr.pushQueue(remain, comp);
        }

        System.out.println(">xxx> pushQueue :: end");

        if (remain > 0) {
            long dataAmount = Math.max(1, requested / subscriptions.size());

            for (Integer idx : minIdxs) {
                requestCompleted.decrementAndGet();

                subscribers.get(idx).onDataRequested(dataAmount);
            }

            for (Integer idx : minIdxs) {
                System.out.println(">xxx> idx=" + idx + " requested=" + dataAmount);

                subscriptions.get(idx).request(dataAmount);
            }
        } else {
            System.out.println(">xxx> remain is zero");
        }
    }

    private List<Integer> selectMinIdx() {
        T minItem = null;
        List<Integer> minIdxs = new ArrayList<>();

        for (int i = 0; i < subscribers.size(); i++) {
            SortingSubscriber<T> subscriber = subscribers.get(i);

            if (subscriber == null || subscriber.finished()) {
                continue;
            }

            T item = subscriber.lastItem();

            int cmpRes = 0;

            if (minItem == null || (cmpRes = comp.compare(minItem, item)) >= 0) {
                minItem = item;

                if (cmpRes != 0) {
                    minIdxs.clear();
                }

                minIdxs.add(i);
            }
        }

        if (minIdxs.isEmpty()) {
            new IllegalStateException("minIdx is empty").printStackTrace();
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

    public void cancel(int idx) {
        System.out.println(">xxx> onComplete " + idx);
        finished.add(idx);

        System.out.println(">xxx> finished " + idx);

        if (requestCompleted.get() >= activeSubcribers()) {
            System.out.println(">xxx> [" + idx + "] cancel -> onRequestCompleted " + activeSubcribers());

            onRequestCompleted0();
        }
        else {
            System.out.println(">xxx> still waiting completed=" + requestCompleted.get() + ", actie =" + activeSubcribers());
        }
    }
}
