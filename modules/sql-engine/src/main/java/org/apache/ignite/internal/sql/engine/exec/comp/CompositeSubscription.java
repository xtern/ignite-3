package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
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

    private final AtomicInteger finishedCnt = new AtomicInteger();

    private final AtomicBoolean completed = new AtomicBoolean();

    volatile long remain = 0;

    volatile long requested = 0;

//    private final AtomicInteger requestCompleted = new AtomicInteger();
    private final Set<Integer> waitResponse = new ConcurrentHashSet<>();

    CompositeSubscription(Comparator<T> comp, PriorityBlockingQueue<T> queue) {
        this.comp = comp;
        this.queue = queue;
    }

    public int activeSubcribers() {
        return subscriptions.size() - finished.size();
    }

    public boolean onRequestCompleted(int idx) {
        // Internal buffers has been filled.
        if (waitResponse.remove(idx) && waitResponse.isEmpty()) {
            onRequestCompleted0();

            return true;
        } else {
            debug(">xxx> waiting " + waitResponse.size());
        }

        return false;
    }

    public synchronized boolean onRequestCompleted0() {
        List<Integer> minIdxs = selectMinIdx();

        if (minIdxs.isEmpty())
            return false;

        // todo
        for (int idx : minIdxs) {
            debug(">xxx> minIdx=" + idx + " lastItem=" + subscribers.get(idx).lastItem());
        }

        SortingSubscriber<T> subscr = subscribers.get(minIdxs.get(0));

//        assert subscr != null && !subscr.finished();

        debug(">xxx> pushQueue :: start, t=" + Thread.currentThread().getId());

        synchronized (this) {
            remain = subscr.pushQueue(remain, comp);
        }

        debug(">xxx> pushQueue :: end");

        if (remain > 0) {
            long dataAmount = Math.max(1, requested / subscriptions.size());

            for (Integer idx : minIdxs) {
                waitResponse.add(idx);

                subscribers.get(idx).onDataRequested(dataAmount);
            }

            for (Integer idx : minIdxs) {
                debug(">xxx> idx=" + idx + " requested=" + dataAmount);

                subscriptions.get(idx).request(dataAmount);
            }
        } else {
            debug(">xxx> remain is zero");
        }

        return true;
    }

    private synchronized List<Integer> selectMinIdx() {
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
        // todo we may return result before all publishers has finished publishing

        while (!waitResponse.isEmpty()) {
            try {
                Thread.sleep(100);

                debug(">xxx> wait for idx=" + waitResponse);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        synchronized (this) {
            remain = n;
            requested = n;
        }

//        waitResponse.clear();

        // Perhaps we can return something from internal buffer?
        if (queue.size() > 0) {
//            List<Integer> minIdxs = selectMinIdx();
            if (finished.size() == subscriptions.size()) { // all data has been received
                if (subscribers.get(0).pushQueue(n, null) == 0)
                    return;
            }
            else { // we have someone alive
                onRequestCompleted0();

                return;
            }
        }

        long requestCnt = Math.max(1, n / subscriptions.size());

        for (int i = 0; i < subscriptions.size(); i++) {
            SortingSubscriber<T> subscriber = subscribers.get(i);

            if (subscriber.finished())
                continue;

            waitResponse.add(i);
            subscriber.onDataRequested(requestCnt);
        }

        for (int i = 0; i< subscriptions.size(); i++) {
            if (subscribers.get(i).finished())
                continue;

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

    public synchronized void cancel(int idx) {
        debug(">xxx> onComplete " + idx);
        finished.add(idx);
        if (finishedCnt.incrementAndGet() == subscriptions.size() && (remain > 0 || queue.size() == 0)) {
            waitResponse.remove(idx);

            if (completed.compareAndSet(false, true)) {
                debug(">xxx> push queue, remain=" + remain + " queue=" + queue.size());
                synchronized (this) {
                    subscribers.get(0).pushQueue(remain, null);
                }
            }

            // all work done
            return;
        }

        onRequestCompleted(idx);

        debug(">xxx> finished " + idx + " t=" + Thread.currentThread().getId());

//        if (requestCompleted.get() >= activeSubcribers()) {
//            debug(">xxx> [" + idx + "] cancel -> onRequestCompleted " + activeSubcribers());
//
//            onRequestCompleted0();
//        }
//        else {
//            debug(">xxx> still waiting completed=" + requestCompleted.get() + ", actie =" + activeSubcribers());
//        }
    }

    private static boolean debug = true;

    private static void debug(String msg) {
        if (debug)
            System.out.println(msg);
    }
}
