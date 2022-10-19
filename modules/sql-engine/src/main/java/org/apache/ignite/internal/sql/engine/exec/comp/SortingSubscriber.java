package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.Comparator;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

class SortingSubscriber<T> implements Subscriber<T> {
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

    SortingSubscriber(Subscriber<T> delegate, int idx, CompositeSubscription<T> compSubscription, PriorityBlockingQueue<T> queue) {
        assert delegate != null;

        this.delegate = delegate;
        this.idx = idx;
        this.compSubscription = compSubscription;
        this.queue = queue;
    }

    T lastItem() {
        return lastItem;
    }

    boolean finished() {
        return finished;
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

            if (!done && same) {
                done = true;
            }

            if (!done || same) {
                delegate.onNext(queue.poll());

                --remain;
            }

            if (done && !same) {
                break;
            }
        }

        if (remain == 0) {
            delegate.onComplete();
        }

        return remain;
    }

    boolean complete() {
        return compSubscription.remove(subscription) && compSubscription.subscriptions().isEmpty();
    }
}
