/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wplatform.ddal.route.algorithm;

import java.util.List;

import com.wplatform.ddal.route.rule.TableNode;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class HashSlotsPartitioner extends CommonPartitioner {

    // 分区长度:数据段分布定义，其中取模的数一定要是2^n， 因为这里使用x % 2^n == x & (2^n - 1)等式，来优化性能。
    private static final int PARTITION_LENGTH = 1024;

    // %转换为&操作的换算数值
    private static final long AND_VALUE = PARTITION_LENGTH - 1;
    
    // 分区线段
    private final int[] segment = new int[PARTITION_LENGTH];
    
    protected int[] count;
    protected int[] length;

    /**
     * <pre>
     * @param count 表示定义的分区数
     * @param length 表示对应每个分区的取值长度
     * 注意：其中count,length两个数组的长度必须是一致的。
     * 约束：1024 = sum((count[i]*length[i])). count和length两个向量的点积恒等于1024
     * </pre>
     */
    @Override
    public void doInit(List<TableNode> tableNodes) {
        super.doInit(tableNodes);
        if (count == null || length == null || (count.length != length.length)) {
            throw new RuntimeException("error,check your scope & scopeLength definition.");
        }
        int segmentLength = 0;
        for (int i = 0; i < count.length; i++) {
            segmentLength += count[i];
        }
        int[] ai = new int[segmentLength + 1];
        int index = 0;
        for (int i = 0; i < count.length; i++) {
            for (int j = 0; j < count[i]; j++) {
                ai[++index] = ai[index - 1] + length[i];
            }
        }
        if (ai[ai.length - 1] != PARTITION_LENGTH) {
            throw new RuntimeException("error,check your partitionScope definition.");
        }
        // 数据映射操作
        for (int i = 1; i < ai.length; i++) {
            for (int j = ai[i - 1]; j < ai[i]; j++) {
                segment[j] = (i - 1);
            }
        }
    }


    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }
    
    private static int[] toIntArray(String string) {
        String[] strs = string.split(string, ',');
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    
    public int partition(long hash) {
        return segment[(int) (hash & AND_VALUE)];
    }


    @Override
    public Integer partition(Value value) {
        // TODO Auto-generated method stub
        return null;
    }

}
