/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;

import java.util.List;
import java.util.Map;

/** Table drop partition action for Flink. */
public class DropPartitionAction extends TableActionBase {

    private final List<Map<String, String>> partitions;
    private final BatchTableCommit commit;

    public DropPartitionAction(
            String databaseName,
            String tableName,
            List<Map<String, String>> partitions,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports drop-partition action. The table type is '%s'.",
                            table.getClass().getName()));
        }

        this.partitions = partitions;
        this.commit = table.newBatchWriteBuilder().newCommit();
    }

    @Override
    public void run() throws Exception {
        commit.truncatePartitions(partitions);
    }
}
