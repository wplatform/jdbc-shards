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

import com.wplatform.ddal.route.rule.RuleEvaluateException;
import com.wplatform.ddal.route.rule.TableNode;
import com.wplatform.ddal.util.MurmurHash;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueNull;

import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class HashBucketPartitioner extends CommonPartitioner {

    private static final int HASH_BUCKET_SIZE = 1 << 10;


    private int[] count;
    private int[] length;
    private PartitionUtil partitionUtil;


    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }

    @Override
    public void initialize(List<TableNode> tableNodes) {
        super.initialize(tableNodes);
        partitionUtil = new PartitionUtil(HASH_BUCKET_SIZE, count, length);
    }

    @Override
    public Integer partition(Value value) {
        if (value == null || value == ValueNull.INSTANCE) {
            return getDefaultNodeIndex();
        }
        int type = value.getType();
        switch (type) {
            case Value.BLOB:
            case Value.CLOB:
            case Value.ARRAY:
            case Value.RESULT_SET:
                throw new RuleEvaluateException("Invalid type for " + getClass().getName());
        }
        byte[] bytes = value.getBytes();
        long hash64 = MurmurHash.hash64(bytes, bytes.length);
        return partitionUtil.partition(hash64);
    }

    public static void main(String[] args) {
        System.out.println(1 << 15);
    }
}
