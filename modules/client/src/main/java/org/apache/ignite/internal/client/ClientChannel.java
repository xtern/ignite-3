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

package org.apache.ignite.internal.client;

import java.util.concurrent.CompletableFuture;

/**
 * Processing thin client requests and responses.
 */
public interface ClientChannel extends AutoCloseable {
    /**
     * Send request and handle response asynchronously for client operation.
     *
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @param payloadReader Payload reader from stream.
     * @param <T>           Response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader
    );

    /**
     * Returns {@code true} channel is closed.
     *
     * @return {@code True} channel is closed.
     */
    public boolean closed();

    /**
     * Returns protocol context.
     *
     * @return Protocol context.
     */
    public ProtocolContext protocolContext();
}
