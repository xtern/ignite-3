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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowConverter;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Scan node.
 * TODO: merge with {@link TableScanNode}
 */
public class IndexScanNode<RowT> extends AbstractNode<RowT> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    private static final int NOT_WAITING = -1;

    /** Schema index. */
    private final IgniteIndex schemaIndex;

    /** Index row layout. */
    private final BinaryTupleSchema indexRowSchema;

    private final RowHandler.RowFactory<RowT> factory;

    private final int[] parts;

    private final Queue<RowT> inBuff = new LinkedBlockingQueue<>(inBufSize);

    private final @Nullable Predicate<RowT> filters;

    private final @Nullable Function<RowT, RowT> rowTransformer;

    /** Participating columns. */
    private final @Nullable BitSet requiredColumns;

    private final @Nullable Supplier<RowT> lowerCond;

    private final @Nullable Supplier<RowT> upperCond;

    private final int flags;

    private int requested;

    private int waiting;

    private boolean inLoop;

    private Subscription activeSubscription;

//    private int curPartIdx;

    private @Nullable BinaryTuple lowerBound;

    private @Nullable BinaryTuple upperBound;

    private Comparator<RowT> cmp;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowType Output type of the current node.
     * @param schemaTable The table this node should scan.
     * @param parts Partition numbers to scan.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     * @param requiredColumns Optional set of column of interest.
     */
    public IndexScanNode(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            IgniteIndex schemaIndex,
            InternalIgniteTable schemaTable,
            int[] parts,
            Comparator<RowT> cmp,
            @Nullable Supplier<RowT> lowerCond,
            @Nullable Supplier<RowT> upperCond,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx, rowType);
        assert !nullOrEmpty(parts);

        this.schemaIndex = schemaIndex;
        this.parts = parts;
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColumns = requiredColumns;

        factory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        indexRowSchema = RowConverter.createIndexRowSchema(schemaTable.descriptor(), schemaIndex.index().descriptor());

        // TODO: create ticket to add flags support
        flags = SortedIndex.INCLUDE_LEFT & SortedIndex.INCLUDE_RIGHT;
        this.cmp = cmp;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::push, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
//        curPartIdx = 0;
        lowerBound = null;
        upperBound = null;

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    private void push() throws Exception {
        if (isClosed()) {
            return;
        }

        checkState();

        if (requested > 0 && !inBuff.isEmpty()) {
            inLoop = true;
            try {
                while (requested > 0 && !inBuff.isEmpty()) {
                    checkState();

                    RowT row = inBuff.poll();

                    if (filters != null && !filters.test(row)) {
                        continue;
                    }

                    if (rowTransformer != null) {
                        row = rowTransformer.apply(row);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }
        }

        if (waiting == 0 || activeSubscription == null) {
            requestNextBatch();
        }

        if (requested > 0 && waiting == NOT_WAITING) {
            if (inBuff.isEmpty()) {
                requested = 0;
                downstream().end();
            } else {
                context().execute(this::push, this::onError);
            }
        }
    }

    private boolean processed = false;

    private void requestNextBatch() {
        if (waiting == NOT_WAITING) {
            return;
        }

        System.out.println("request next");

        if (waiting == 0) {
            // we must not request rows more than inBufSize
            waiting = inBufSize - inBuff.size();
        }

        Subscription subscription = this.activeSubscription;
        if (subscription != null) {
            subscription.request(waiting);

            waiting = NOT_WAITING;

            return;
        }

        if (processed) {
            waiting = NOT_WAITING;

            processed = false;

            return;
        }

        CompositePublisher publisher = new CompositePublisher();

        for (int curPartIdx = 0; curPartIdx < parts.length; curPartIdx++) {

//        else if (curPartIdx < parts.length) {
            if (schemaIndex.type() == Type.SORTED) {
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17813
                // Introduce new publisher using merge-sort algo to merge partition index publishers.
                if (lowerBound == null && upperBound == null) {
                    lowerBound = toBinaryTuplePrefix(lowerCond);
                    upperBound = toBinaryTuplePrefix(upperCond);
                }

                publisher.add(((SortedIndex) schemaIndex.index()).scan(
                        parts[curPartIdx],
                        context().transaction(),
                        lowerBound,
                        upperBound,
                        flags,
                        requiredColumns
                ));
            } else {
                assert schemaIndex.type() == Type.HASH;
                assert lowerCond == upperCond;

                if (lowerBound == null) {
                    lowerBound = toBinaryTuple(lowerCond);
                }

                publisher.add(
                        schemaIndex.index().scan(
                                parts[curPartIdx],
                                context().transaction(),
                                lowerBound,
                                requiredColumns
                        ));
            }
        }

        processed = true;

        publisher.subscribe(new SubscriberImpl());
//        } else {
//            waiting = NOT_WAITING;
//        }
    }

    class MagicSubscriber<T> implements Flow.Subscriber<T> {

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
            if (complete())
                delegate.onError(throwable);
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

    private class CompositePublisher implements Flow.Publisher<BinaryTuple> {

        List<Flow.Publisher<BinaryTuple>> publishers = new ArrayList<>();

        CompositeSubscription compSubscription = new CompositeSubscription();

//        List<Flow.Subscriber<? super BinaryTuple>> susbcribers = new ArrayList<>();

        AtomicBoolean subscribed = new AtomicBoolean();

//        Flow.Subscriber<? super BinaryTuple> finalSubscriber;

        public void add(Flow.Publisher<BinaryTuple> publisher) {
            publishers.add(publisher);
        }

//        CompositeSubscriber compSubscr = new CompositeSubscriber();

        @Override
        public void subscribe(Subscriber<? super BinaryTuple> subscriber) {
            // todo sync
            if (!subscribed.compareAndSet(false, true))
                throw new IllegalStateException("Support only one subscriber");

            for (int i = 0; i < publishers.size(); i++) {
                publishers.get(i).subscribe(new MagicSubscriber<>(subscriber, i, compSubscription));
            }

            subscriber.onSubscribe(compSubscription);
        }
    }

    private class CompositeSubscription implements Flow.Subscription {

        private List<Flow.Subscription> subscriptions = new ArrayList<>();

        public List<Flow.Subscription> subscriptions() {
            return subscriptions;
        }

        public void add(Flow.Subscription subscription) {
            subscriptions.add(subscription);
        }

        // todo sync
        public boolean remove(Flow.Subscription subscription) {
            return subscriptions.remove(subscription);
        }

        @Override
        public void request(long n) {
            // todo sync
            for (Flow.Subscription subscription : subscriptions) {
                subscription.request(n);
            }
        }

        @Override
        public void cancel() {
            // todo sync
            for (Flow.Subscription subscription : subscriptions) {
                subscription.cancel();
            }
        }
    }
//
//    private class CompositeSubscriber implements Flow.Subscriber<BinaryTuple> {
//
//        List<Flow.Subscriber<? super BinaryTuple>> susbcribers = new ArrayList<>();
//
//        CompositeSubscription compSubscription = new CompositeSubscription();
//
//        public void add(Subscriber<? super BinaryTuple> subscriber) {
//            susbcribers.add(subscriber);
//        }
//
//        List<Flow.Subscriber<? super BinaryTuple>> subscribers() {
//            return susbcribers;
//        }
//
//        @Override
//        public void onSubscribe(Subscription subscription) {
//            compSubscription.add(subscription);
//        }
//
//        @Override
//        public void onNext(BinaryTuple item) {
//
//        }
//
//        @Override
//        public void onError(Throwable throwable) {
//
//        }
//
//        @Override
//        public void onComplete() {
//
//        }
//    }

    private class SubscriberImpl implements Flow.Subscriber<BinaryTuple> {

        private int received = 0; // HB guarded here.

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            assert IndexScanNode.this.activeSubscription == null;

            IndexScanNode.this.activeSubscription = subscription;
            subscription.request(waiting);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(BinaryTuple binRow) {
            RowT row = convert(binRow);

            inBuff.add(row);

            if (++received == inBufSize) {
                received = 0;

                context().execute(() -> {
                    waiting = 0;
                    push();
                }, IndexScanNode.this::onError);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            context().execute(() -> {
                throw throwable;
            }, IndexScanNode.this::onError);
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            context().execute(() -> {
                activeSubscription = null;
                waiting = 0;

                push();
            }, IndexScanNode.this::onError);
        }
    }

    @Contract("null -> null")
    private @Nullable BinaryTuple toBinaryTuplePrefix(@Nullable Supplier<RowT> conditionSupplier) {
        if (conditionSupplier == null) {
            return null;
        }

        return RowConverter.toBinaryTuplePrefix(context(), indexRowSchema, factory, conditionSupplier.get());
    }

    @Contract("null -> null")
    private @Nullable BinaryTuple toBinaryTuple(@Nullable Supplier<RowT> conditionSupplier) {
        if (conditionSupplier == null) {
            return null;
        }

        return RowConverter.toBinaryTuple(context(), indexRowSchema, factory, conditionSupplier.get());
    }

    private RowT convert(BinaryTuple binaryTuple) {
        return RowConverter.toRow(context(), binaryTuple, factory);
    }
}
