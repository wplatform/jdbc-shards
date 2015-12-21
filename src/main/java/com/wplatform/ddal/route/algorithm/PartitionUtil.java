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


public final class PartitionUtil {

    private final int partitionLength;

    private final long andValue;

    private final int[] segment;

    /**
     * partitionLength must be 2^n
     * partitionLength = sum((count[i]*length[i]))
     * @param partitionLength
     * @param count
     * @param length
     */
    public PartitionUtil(int partitionLength, int[] count, int[] length) {
        if (partitionLength < 1 || partitionLength > 32768) {
            throw new IllegalArgumentException("partitionLength must be between 0 and 32768");
        }
        if ((partitionLength & partitionLength - 1) != 0) {
            throw new IllegalArgumentException("partitionLength must be 2^n");
        }
        if (count == null || length == null || (count.length != length.length)) {
            throw new IllegalArgumentException("error,check your scope & scopeLength definition.");
        }
        this.partitionLength = partitionLength;
        this.andValue = partitionLength - 1;
        this.segment = new int[partitionLength];
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
        if (ai[ai.length - 1] != this.partitionLength) {
            throw new IllegalArgumentException("error,check your partitionScope definition.");
        }
        // 数据映射操作
        for (int i = 1; i < ai.length; i++) {
            for (int j = ai[i - 1]; j < ai[i]; j++) {
                segment[j] = (i - 1);
            }
        }
    }

    /**
     * if x is 2^n，x % 2^n == x & (2^n - 1)
     *
     * @param hash
     * @return
     */
    public int partition(long hash) {
        int index = (int) (hash & andValue);
        return segment[index];
    }

}
