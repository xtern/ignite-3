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

package org.apache.ignite.internal.sql.engine.rule;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSingleIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSingleMinus;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;

/**
 * Set op (MINUS, INTERSECT) converter rule.
 */
public class SetOpConverterRule {
    public static final RelOptRule SINGLE_MINUS = new SingleMinusConverterRule();

    public static final RelOptRule SINGLE_INTERSECT = new SingleIntersectConverterRule();

    public static final RelOptRule MAP_REDUCE_MINUS = new MapReduceMinusConverterRule();

    public static final RelOptRule MAP_REDUCE_INTERSECT = new MapReduceIntersectConverterRule();

    private SetOpConverterRule() {
        // No-op.
    }

    private abstract static class SingleSetOpConverterRule<T extends SetOp> extends AbstractIgniteConverterRule<T> {
        SingleSetOpConverterRule(Class<T> cls, String desc) {
            super(cls, desc);
        }

        /** Node factory method. */
        abstract PhysicalNode createNode(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all);

        /** {@inheritDoc} */
        @Override
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, T setOp) {
            RelOptCluster cluster = setOp.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            List<RelNode> inputs = Util.transform(setOp.getInputs(), rel -> convert(rel, inTrait));

            return createNode(cluster, outTrait, inputs, setOp.all);
        }
    }

    private static class SingleMinusConverterRule extends SingleSetOpConverterRule<LogicalMinus> {
        SingleMinusConverterRule() {
            super(LogicalMinus.class, "SingleMinusConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        PhysicalNode createNode(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
                boolean all) {
            return new IgniteSingleMinus(cluster, traits, inputs, all);
        }
    }

    private static class SingleIntersectConverterRule extends SingleSetOpConverterRule<LogicalIntersect> {
        SingleIntersectConverterRule() {
            super(LogicalIntersect.class, "SingleIntersectConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        PhysicalNode createNode(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
                boolean all) {
            return new IgniteSingleIntersect(cluster, traits, inputs, all);
        }
    }

    private abstract static class MapReduceSetOpConverterRule<T extends SetOp> extends AbstractIgniteConverterRule<T> {
        MapReduceSetOpConverterRule(Class<T> cls, String desc) {
            super(cls, desc);
        }

        /** Map node factory method. */
        abstract PhysicalNode createMapNode(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
                boolean all);

        /** Reduce node factory method. */
        abstract PhysicalNode createReduceNode(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                boolean all, RelDataType rowType);

        /** {@inheritDoc} */
        @Override
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, T setOp) {
            RelOptCluster cluster = setOp.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            List<RelNode> inputs = Util.transform(setOp.getInputs(), rel -> convert(rel, inTrait));

            RelNode map = createMapNode(cluster, outTrait, inputs, setOp.all);

            return createReduceNode(
                    cluster,
                    outTrait.replace(IgniteDistributions.single()),
                    convert(map, inTrait.replace(IgniteDistributions.single())),
                    setOp.all,
                    cluster.getTypeFactory().leastRestrictive(Util.transform(inputs, RelNode::getRowType))
            );
        }
    }

    private static class MapReduceMinusConverterRule extends MapReduceSetOpConverterRule<LogicalMinus> {
        MapReduceMinusConverterRule() {
            super(LogicalMinus.class, "MapReduceMinusConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        PhysicalNode createMapNode(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
                boolean all) {
            return new IgniteMapMinus(cluster, traits, inputs, all);
        }

        /** {@inheritDoc} */
        @Override
        PhysicalNode createReduceNode(RelOptCluster cluster, RelTraitSet traits, RelNode input, boolean all,
                RelDataType rowType) {
            return new IgniteReduceMinus(cluster, traits, input, all, rowType);
        }
    }

    private static class MapReduceIntersectConverterRule extends MapReduceSetOpConverterRule<LogicalIntersect> {
        MapReduceIntersectConverterRule() {
            super(LogicalIntersect.class, "MapReduceIntersectConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        PhysicalNode createMapNode(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs,
                boolean all) {
            return new IgniteMapIntersect(cluster, traits, inputs, all);
        }

        /** {@inheritDoc} */
        @Override
        PhysicalNode createReduceNode(RelOptCluster cluster, RelTraitSet traits, RelNode input, boolean all,
                RelDataType rowType) {
            return new IgniteReduceIntersect(cluster, traits, input, all, rowType);
        }
    }
}
