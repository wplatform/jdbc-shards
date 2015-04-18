/*
 * Copyright 2015 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2015年4月13日
// $Id$

package com.suning.snfddal.shards;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import com.suning.snfddal.util.MurmurHash;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class LoadBalance {
    private static final int defaultNumberOfReplicas = 16;
    private int numberOfReplicas;
    private SortedMap<Integer, String> circle = new TreeMap<Integer, String>();

    public LoadBalance(Collection<String> nodes, int numberOfReplicas) {
        if (numberOfReplicas < 1) {
            throw new IllegalArgumentException("The numberOfReplicas must bigger then 0.");
        }
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("The shards can't empty.");
        }
        this.numberOfReplicas = numberOfReplicas;
        for (String node : nodes) {
            add(node);
        }
    }

    public LoadBalance(Collection<String> nodes) {
        this(nodes, defaultNumberOfReplicas);
    }

    public void add(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String decorateWithCounter = decorateWithCounter(node.toString(), i);
            circle.put(hash(decorateWithCounter), node);
        }
    }

    public void remove(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hash(node.toString() + i));
        }
    }

    public String get(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hash(key.toString());
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private static int hash(final String k) {
        return MurmurHash.hash32(k);
    }

    private static String decorateWithCounter(final String input, final int counter) {
        return new StringBuilder(input).append('%').append(counter).append('%').toString();
    }
}
