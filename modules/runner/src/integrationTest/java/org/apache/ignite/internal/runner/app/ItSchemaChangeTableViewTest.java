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

package org.apache.ignite.internal.runner.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

/**
 * Ignition interface tests.
 */
class ItSchemaChangeTableViewTest extends AbstractSchemaChangeTest {
    /**
     * Check add a new column to table schema.
     */
    @Test
    public void testDropColumn() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", 111).set("valStr", "str"));

        dropColumn(grid, "valStr");

        // Check old row conversion.
        final Tuple keyTuple = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(null, keyTuple).value("key"));
        assertEquals(111, (Integer) tbl.get(null, keyTuple).value("valInt"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(null, keyTuple).value("valStr"));

        // Check tuple of outdated schema.
        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(null, Tuple.create().set("key", 2L).set("valInt", -222).set("valStr", "str"))
        );

        // Check tuple of correct schema.
        tbl.insert(null, Tuple.create().set("key", 2L).set("valInt", 222));

        final Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(null, keyTuple2).value("key"));
        assertEquals(222, (Integer) tbl.get(null, keyTuple2).value("valInt"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(null, keyTuple2).value("valStr"));
    }

    /**
     * Check drop column from table schema.
     */
    @Test
    public void testAddNewColumn() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", 111));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", -111).set("valStrNew", "str"))
        );

        addColumn(grid, SchemaBuilders.column("valStrNew", ColumnType.string())
                .asNullable(true).withDefaultValue("default").build());

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(null, keyTuple1).value("key"));
        assertEquals(111, (Integer) tbl.get(null, keyTuple1).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple1).value("valStrNew"));

        // Check tuple of new schema.
        tbl.insert(null, Tuple.create().set("key", 2L).set("valInt", 222).set("valStrNew", "str"));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(null, keyTuple2).value("key"));
        assertEquals(222, (Integer) tbl.get(null, keyTuple2).value("valInt"));
        assertEquals("str", tbl.get(null, keyTuple2).value("valStrNew"));
    }

    /**
     * Check column renaming.
     */
    @Test
    void testRenameColumn() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", 111));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(null, Tuple.create().set("key", 2L).set("valRenamed", -222))
        );

        renameColumn(grid, "valInt", "valRenamed");

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(null, keyTuple1).value("key"));
        assertEquals(111, (Integer) tbl.get(null, keyTuple1).value("valRenamed"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(null, keyTuple1).value("valInt"));

        // Check tuple of outdated schema.
        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(null, Tuple.create().set("key", 2L).set("valInt", -222))
        );

        // Check tuple of correct schema.
        tbl.insert(null, Tuple.create().set("key", 2L).set("valRenamed", 222));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(null, keyTuple2).value("key"));
        assertEquals(222, (Integer) tbl.get(null, keyTuple2).value("valRenamed"));
        assertThrows(IllegalArgumentException.class, () -> tbl.get(null, keyTuple2).value("valInt"));
    }

    /**
     * Rename column then add a new column with same name.
     */
    @Test
    void testRenameThenAddColumnWithSameName() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", 111));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(null, Tuple.create().set("key", 2L).set("val2", -222))
        );

        renameColumn(grid, "valInt", "val2");
        addColumn(grid, SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true)
                .withDefaultValue(-1).build());

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(1, (Long) tbl.get(null, keyTuple1).value("key"));
        assertEquals(111, (Integer) tbl.get(null, keyTuple1).value("val2"));
        assertEquals(-1, (Integer) tbl.get(null, keyTuple1).value("valInt"));

        // Check tuple of outdated schema.
        assertNull(tbl.get(null, Tuple.create().set("key", 2L)));

        // Check tuple of correct schema.
        tbl.insert(null, Tuple.create().set("key", 2L).set("val2", 222));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(2, (Long) tbl.get(null, keyTuple2).value("key"));
        assertEquals(222, (Integer) tbl.get(null, keyTuple2).value("val2"));
        assertEquals(-1, (Integer) tbl.get(null, keyTuple2).value("valInt"));
    }

    /**
     * Check merge table schema changes.
     */
    @Test
    public void testMergeChangesAddDropAdd() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        final ColumnDefinition column = SchemaBuilders.column("val", ColumnType.string()).asNullable(true)
                .withDefaultValue("default")
                .build();

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", 111));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(
                        null,
                        Tuple.create().set("key", 2L).set("val", "I'not exists")
                )
        );

        addColumn(grid, column);

        assertNull(tbl.get(null, Tuple.create().set("key", 2L)));

        tbl.insert(null, Tuple.create().set("key", 2L).set("valInt", 222).set("val", "string"));

        tbl.insert(null, Tuple.create().set("key", 3L).set("valInt", 333));

        dropColumn(grid, column.name());

        tbl.insert(null, Tuple.create().set("key", 4L).set("valInt", 444));

        assertThrowsWithCause(SchemaMismatchException.class,
                () -> tbl.insert(
                        null,
                        Tuple.create().set("key", 4L).set("val", "I'm not exist")
                )
        );

        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValue("default").build());

        tbl.insert(null, Tuple.create().set("key", 5L).set("valInt", 555));

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(111, (Integer) tbl.get(null, keyTuple1).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple1).value("val"));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(222, (Integer) tbl.get(null, keyTuple2).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple2).value("val"));

        Tuple keyTuple3 = Tuple.create().set("key", 3L);

        assertEquals(333, (Integer) tbl.get(null, keyTuple3).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple3).value("val"));

        Tuple keyTuple4 = Tuple.create().set("key", 4L);

        assertEquals(444, (Integer) tbl.get(null, keyTuple4).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple4).value("val"));

        Tuple keyTuple5 = Tuple.create().set("key", 5L);

        assertEquals(555, (Integer) tbl.get(null, keyTuple5).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple5).value("val"));
    }

    /**
     * Check merge column default value changes.
     */
    @Test
    public void testMergeChangesColumnDefault() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        RecordView<Tuple> tbl = grid.get(0).tables().table(TABLE).recordView();

        final String colName = "valStr";

        tbl.insert(null, Tuple.create().set("key", 1L).set("valInt", 111));

        changeDefault(grid, colName, (Supplier<Object> & Serializable) () -> "newDefault");
        addColumn(grid, SchemaBuilders.column("val", ColumnType.string()).withDefaultValue("newDefault").build());

        tbl.insert(null, Tuple.create().set("key", 2L).set("valInt", 222));

        changeDefault(grid, colName, (Supplier<Object> & Serializable) () -> "brandNewDefault");
        changeDefault(grid, "val", (Supplier<Object> & Serializable) () -> "brandNewDefault");

        tbl.insert(null, Tuple.create().set("key", 3L).set("valInt", 333));

        // Check old row conversion.
        Tuple keyTuple1 = Tuple.create().set("key", 1L);

        assertEquals(111, (Integer) tbl.get(null, keyTuple1).value("valInt"));
        assertEquals("default", tbl.get(null, keyTuple1).value("valStr"));
        assertEquals("newDefault", tbl.get(null, keyTuple1).value("val"));

        Tuple keyTuple2 = Tuple.create().set("key", 2L);

        assertEquals(222, (Integer) tbl.get(null, keyTuple2).value("valInt"));
        assertEquals("newDefault", tbl.get(null, keyTuple2).value("valStr"));
        assertEquals("newDefault", tbl.get(null, keyTuple2).value("val"));

        Tuple keyTuple3 = Tuple.create().set("key", 3L);

        assertEquals(333, (Integer) tbl.get(null, keyTuple3).value("valInt"));
        assertEquals("brandNewDefault", tbl.get(null, keyTuple3).value("valStr"));
        assertEquals("brandNewDefault", tbl.get(null, keyTuple3).value("val"));
    }

    /**
     * Check operation failed if unknown column found.
     */
    @Test
    public void testStrictSchemaInsertRowOfNewSchema() throws Exception {
        List<Ignite> grid = startGrid();

        createTable(grid);

        Table tbl = grid.get(0).tables().table(TABLE);

        Tuple tuple = Tuple.create().set("key", 1L).set("unknownColumn", 10);

        assertThrowsWithCause(SchemaMismatchException.class, () -> tbl.recordView().insert(null, tuple));
    }
}
