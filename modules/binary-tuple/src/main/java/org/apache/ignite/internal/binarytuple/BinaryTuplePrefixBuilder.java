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

package org.apache.ignite.internal.binarytuple;

import java.nio.ByteBuffer;

/**
 * Class for build Binary Tuple Prefixes.
 *
 * <p>A Binary Tuple Prefix is an extension of the Binary Tuple format, that is, it adds an additional field, containing the number of
 * elements in the prefix, at the end of the serialized tuple representation. This is helpful in cases when it is required to de-serialize
 * such prefix while only having the full tuple schema.
 *
 * <p>The builder also sets the {@link BinaryTupleCommon#PREFIX_FLAG} in the flags region in order to able to distinguish such prefixes
 * from regular tuples.
 */
public class BinaryTuplePrefixBuilder extends BinaryTupleBuilder {
    private final int prefixNumElements;

    /**
     * Creates a new builder.
     *
     * @param prefixNumElements Number of elements in the prefix.
     * @param fullNumElements Number of elements in the Binary Tuple Schema.
     */
    public BinaryTuplePrefixBuilder(int prefixNumElements, int fullNumElements) {
        super(fullNumElements + 1, true, -1);

        this.prefixNumElements = prefixNumElements;
    }

    @Override
    public ByteBuffer build() {
        int elementIndex = elementIndex();

        if (elementIndex != prefixNumElements) {
            throw new IllegalStateException(String.format(
                    "Unexpected amount of elements in a BinaryTuple prefix. Expected: %d, actual %d",
                    prefixNumElements, elementIndex
            ));
        }

        int numElements = numElements();

        // Use nulls instead of the missing elements.
        while (elementIndex() < numElements - 1) {
            appendNull();
        }

        appendInt(prefixNumElements);

        ByteBuffer tuple = super.build();

        // Set the flag indicating that this tuple is a prefix.
        byte flags = tuple.get(0);

        flags |= BinaryTupleCommon.PREFIX_FLAG;

        tuple.put(0, flags);

        return tuple;
    }
}
