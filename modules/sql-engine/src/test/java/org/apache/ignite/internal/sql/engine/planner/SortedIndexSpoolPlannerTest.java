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

package org.apache.ignite.internal.sql.engine.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.junit.jupiter.api.Test;

/**
 * SortedIndexSpoolPlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SortedIndexSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join on not colocated fields. CorrelatedNestedLoopJoinTest is applicable for this case only with
     * IndexSpool.
     */
    @Test
    public void testNotColocatedEqJoin() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
                "T0",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.affinity(0, "T0", "hash");
                    }
                }
                        .addIndex("t0_jid_idx", 1, 0)
        );

        publicSchema.addTable(
                "T1",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.affinity(0, "T1", "hash");
                    }
                }
                        .addIndex("t1_jid_idx", 1, 0)
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );

        IgniteSortedIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteSortedIndexSpool.class));

        List<RexNode> lowerBound = idxSpool.indexCondition().lowerBound();

        assertNotNull(lowerBound);
        assertEquals(3, lowerBound.size());

        assertTrue(((RexLiteral) lowerBound.get(0)).isNull());
        assertTrue(((RexLiteral) lowerBound.get(2)).isNull());
        assertTrue(lowerBound.get(1) instanceof RexFieldAccess);

        List<RexNode> upperBound = idxSpool.indexCondition().upperBound();

        assertNotNull(upperBound);
        assertEquals(3, upperBound.size());

        assertTrue(((RexLiteral) upperBound.get(0)).isNull());
        assertTrue(((RexLiteral) upperBound.get(2)).isNull());
        assertTrue(upperBound.get(1) instanceof RexFieldAccess);
    }

    /**
     * Check case when exists index (collation) isn't applied not for whole join condition but may be used by part of
     * condition.
     */
    @Test
    public void testPartialIndexForCondition() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
                "T0",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID0", f.createJavaType(Integer.class))
                                .add("JID1", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.affinity(0, "T0", "hash");
                    }
                }
        );

        publicSchema.addTable(
                "T1",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID0", f.createJavaType(Integer.class))
                                .add("JID1", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.affinity(0, "T1", "hash");
                    }
                }
                        .addIndex("t1_jid0_idx", 1, 0)
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid0 = t1.jid0 and t0.jid1 = t1.jid1";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );

        System.out.println("+++ \n" + RelOptUtil.toString(phys));

        IgniteSortedIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteSortedIndexSpool.class));

        List<RexNode> lowerBound = idxSpool.indexCondition().lowerBound();

        assertNotNull(lowerBound);
        assertEquals(4, lowerBound.size());

        assertTrue(((RexLiteral) lowerBound.get(0)).isNull());
        assertTrue(((RexLiteral) lowerBound.get(2)).isNull());
        assertTrue(((RexLiteral) lowerBound.get(3)).isNull());
        assertTrue(lowerBound.get(1) instanceof RexFieldAccess);

        List<RexNode> upperBound = idxSpool.indexCondition().upperBound();

        assertNotNull(upperBound);
        assertEquals(4, upperBound.size());

        assertTrue(((RexLiteral) upperBound.get(0)).isNull());
        assertTrue(((RexLiteral) lowerBound.get(2)).isNull());
        assertTrue(((RexLiteral) lowerBound.get(3)).isNull());
        assertTrue(upperBound.get(1) instanceof RexFieldAccess);
    }

    /**
     * Check colocated fields with DESC ordering.
     */
    @Test
    public void testDescFields() throws Exception {
        IgniteSchema publicSchema = createSchema(
                createTable("T0", 10, IgniteDistributions.affinity(0, "T0", "hash"),
                        "ID", Integer.class, "JID", Integer.class, "VAL", String.class)
                        .addIndex("t0_jid_idx", 1),
                createTable("T1", 100, IgniteDistributions.affinity(0, "T1", "hash"),
                        "ID", Integer.class, "JID", Integer.class, "VAL", String.class)
                        .addIndex(RelCollations.of(TraitUtils.createFieldCollation(1, ColumnCollation.DESC_NULLS_FIRST)), "t1_jid_idx")
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t1.jid < t0.jid";

        assertPlan(sql, publicSchema,
                isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                        .and(input(1, isInstanceOf(IgniteSortedIndexSpool.class)
                                .and(spool -> {
                                    List<RexNode> lowerBound = spool.indexCondition().lowerBound();

                                    // Condition is LESS_THEN, but we have DESC field and condition should be in lower bound
                                    // instead of upper bound.
                                    assertNotNull(lowerBound);
                                    assertEquals(3, lowerBound.size());

                                    assertTrue(((RexLiteral) lowerBound.get(0)).isNull());
                                    assertTrue(lowerBound.get(1) instanceof RexFieldAccess);
                                    assertTrue(((RexLiteral) lowerBound.get(2)).isNull());

                                    List<RexNode> upperBound = spool.indexCondition().upperBound();

                                    assertNull(upperBound);

                                    return true;
                                })
                                .and(hasChildThat(isIndexScan("T1", "t1_jid_idx")))
                        )),
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );
    }
}
