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
package com.wplatform.ddal.shards;

import java.util.Collection;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import com.wplatform.ddal.util.MurmurHash;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ConsistentHashing implements LoadBalancingStrategy {


    private static final int defaultNumberOfReplicas = 16;
    private int numberOfReplicas = defaultNumberOfReplicas;
    private Random random = new Random();
    private boolean readOnly;
    private SortedMap<Integer, DataSourceMarker> circle = new TreeMap<Integer, DataSourceMarker>();

    public ConsistentHashing(Collection<DataSourceMarker> nodes, boolean readOnly) {
        if (numberOfReplicas < 1) {
            throw new IllegalArgumentException("The numberOfReplicas must bigger then 0.");
        }
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("The shards can't empty.");
        }
        this.readOnly = readOnly;
        for (DataSourceMarker node : nodes) {
            add(node);
        }
    }

    private static int hash(final String k) {
        return MurmurHash.hash32(k);
    }

    private static String decorateWithCounter(final String input, final int counter) {
        return new StringBuilder(input).append('%').append(counter).append('%').toString();
    }

    public void add(DataSourceMarker node) {
        int weight = readOnly ? node.getrWeight() : node.getwWeight();
        int nodeCount = numberOfReplicas * weight;
        for (int i = 0; i < nodeCount; i++) {
            String decorateWithCounter = decorateWithCounter(node.toString(), i);
            circle.put(hash(decorateWithCounter), node);
        }
    }

    public void remove(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hash(node.toString() + i));
        }
    }

    public DataSourceMarker get(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hash(key.toString());
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, DataSourceMarker> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    public boolean hasNodes() {
        return circle.isEmpty();
    }

    
    @Override
    public DataSourceMarker next() {
        return get(random.nextInt());
    }

}
