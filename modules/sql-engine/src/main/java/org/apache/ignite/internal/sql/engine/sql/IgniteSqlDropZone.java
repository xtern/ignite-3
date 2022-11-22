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

package org.apache.ignite.internal.sql.engine.sql;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code DROP ZONE} statement.
 */
public class IgniteSqlDropZone extends SqlDrop {
    /** Index name. */
    private final SqlIdentifier zoneName;

    /** Sql operator. */
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP ZONE", SqlKind.OTHER);

    /** Constructor. */
    public IgniteSqlDropZone(SqlParserPos pos, boolean ifExists, SqlIdentifier zoneName) {
        super(OPERATOR, pos, ifExists);
        this.zoneName = Objects.requireNonNull(zoneName, "zone name");
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(zoneName);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName()); // "DROP ..."

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }

        zoneName.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier zoneName() {
        return zoneName;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
