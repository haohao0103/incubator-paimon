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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.deletionvectors.ApplyDeletionFileRecordIterator;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.utils.IntArrayList;

import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.IOException;

/** To convert {@link VectorizedColumnBatch} to Arrow format. */
public class ArrowVectorizedBatchConverter extends ArrowBatchConverter {

    private VectorizedColumnBatch batch;
    private @Nullable int[] pickedInColumn;
    private int totalNumRows;
    private int startIndex;

    public ArrowVectorizedBatchConverter(VectorSchemaRoot root, ArrowFieldWriter[] fieldWriters) {
        super(root, fieldWriters);
    }

    /**
     * 将当前批次的数据写入目标位置
     *
     * @param maxBatchRows 每批次允许写入的最大行数。实际写入行数取该值与剩余未处理行数的最小值
     *     <p>函数逻辑： 1. 计算本次实际需要写入的行数 2. 遍历所有列向量，通过对应的字段写入器进行数据写入 3. 更新根节点的行计数 4.
     *     维护写入进度索引，当所有数据处理完成后释放迭代器资源
     */
    @Override
    public void doWrite(int maxBatchRows) {
        // 计算本批次实际写入行数（不超过剩余待处理行数）
        int batchRows = Math.min(maxBatchRows, totalNumRows - startIndex);

        // 获取当前批次的所有列向量并进行遍历处理
        ColumnVector[] columns = batch.columns;
        for (int i = 0; i < columns.length; i++) {
            // 调用对应字段的写入器执行具体列数据写入
            fieldWriters[i].write(columns[i], pickedInColumn, startIndex, batchRows);
        }

        // 更新根节点的行计数为本次实际写入行数
        root.setRowCount(batchRows);

        // 维护写入进度索引
        startIndex += batchRows;

        // 当全部数据处理完成时释放迭代器资源
        if (startIndex >= totalNumRows) {
            releaseIterator();
        }
    }

    public void reset(ApplyDeletionFileRecordIterator iterator) {
        this.iterator = iterator;

        FileRecordIterator<InternalRow> innerIterator = iterator.iterator();
        this.batch = ((VectorizedRecordIterator) innerIterator).batch();

        try {
            DeletionVector deletionVector = iterator.deletionVector();
            int originNumRows = this.batch.getNumRows();
            IntArrayList picked = new IntArrayList(originNumRows);
            for (int i = 0; i < originNumRows; i++) {
                innerIterator.next();
                long returnedPosition = innerIterator.returnedPosition();
                if (!deletionVector.isDeleted(returnedPosition)) {
                    picked.add(i);
                }
            }

            if (picked.size() == originNumRows) {
                this.pickedInColumn = null;
                this.totalNumRows = originNumRows;
            } else {
                this.pickedInColumn = picked.toArray();
                this.totalNumRows = this.pickedInColumn.length;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to apply deletion vector.", e);
        }

        this.startIndex = 0;
    }

    public void reset(VectorizedRecordIterator iterator) {
        this.iterator = iterator;
        this.batch = iterator.batch();
        this.pickedInColumn = null;
        this.totalNumRows = this.batch.getNumRows();
        this.startIndex = 0;
    }
}
