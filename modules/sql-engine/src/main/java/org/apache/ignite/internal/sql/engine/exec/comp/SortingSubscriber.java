package org.apache.ignite.internal.sql.engine.exec.comp;

import java.util.Comparator;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

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

    private final AtomicBoolean finished = new AtomicBoolean();

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
        return finished.get();
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
                throw new IllegalStateException("!!!!remaining failed " + remainingCnt.get());
            }

            compSubscription.onRequestCompleted(idx);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        // todo
        throwable.printStackTrace();

        compSubscription.cancel();

        delegate.onError(throwable);
    }

    public void onDataRequested(long n) {
        // todo add assertion?
        if (finished.get())
            return;

        remainingCnt.set(n);
    }

    @Override
    public void onComplete() {
        if (finished.compareAndSet(false, true)) {
//            throw new IllegalStateException("Second on complete");
//            finished.set(true);
            remainingCnt.set(0);

            compSubscription.cancel(idx);
        }
        // last submitter will choose what to do next
//            compSubscription.onRequestCompleted();

//            debug(">xxx> complete " + idx);
        // todo sync properly
//            if (complete()) {
////                debug(">xxx> completed");
//                delegate.onComplete();
//            }
    }

    public long pushQueue(long remain, @Nullable Comparator<T> comp) {
        boolean done = false;
        int pushedCnt = 0;
        T r = null;

//        if (remain == 0 || queue.isEmpty())
//            return 0;

//        assert lastItem != null;

        while (remain > 0 && (r = queue.peek()) != null) {
            int cmpRes = comp == null ? 0 : comp.compare(lastItem, r);
//            if (comp != null) {
//
//
//            debug(">xxx> lastItem=" + lastItem + " r=" + r + " res = " + cmpRes);
            if (cmpRes < 0) {

                return remain;
            }
//            }

            boolean same = comp != null && cmpRes == 0;

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

        if (comp == null && queue.isEmpty()) {
            delegate.onComplete();
        }

        return remain;
    }

    private static boolean debug = true;

    private static void debug(String msg) {
        if (debug)
            debug(msg);
    }
}
