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
package com.wplatform.ddal.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.wplatform.ddal.value.CaseInsensitiveMap;

/**
 * This class contains static methods to construct commonly used generic objects
 * such as ArrayList.
 */
public class New {

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> ArrayList<T> arrayList() {
        return new ArrayList<T>(4);
    }

    /**
     * Create a new HashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap() {
        return new HashMap<K, V>();
    }

    /**
     * Create a new HashSet.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> HashSet<T> hashSet() {
        return new HashSet<T>();
    }

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @param c   the collection
     * @return the object
     */
    public static <T> ArrayList<T> arrayList(Collection<T> c) {
        return new ArrayList<T>(c);
    }

    /**
     * Create a new ArrayList.
     *
     * @param <T>             the type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <T> ArrayList<T> arrayList(int initialCapacity) {
        return new ArrayList<T>(initialCapacity);
    }

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> LinkedList<T> linkedList() {
        return new LinkedList<T>();
    }

    /**
     * Create a new CopyOnWriteArrayList.
     *
     * @param <T>             the type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <T> CopyOnWriteArrayList<T> copyOnWriteArrayList() {
        return new CopyOnWriteArrayList<T>();
    }

    /**
     * Create a new HashMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap(int initialCapacity) {
        return new HashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new HashMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap(int initialCapacity, float loadFactor) {
        return new HashMap<K, V>(initialCapacity, loadFactor);
    }

    /**
     * Create a new LinkedHashMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> LinkedHashMap<K, V> linkedHashMap() {
        return new LinkedHashMap<K, V>();
    }

    /**
     * Create a new LinkedHashMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> LinkedHashMap<K, V> linkedHashMap(int initialCapacity) {
        return new LinkedHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new LinkedHashMap.
     *
     * @param initialCapacity
     * @param loadFactor
     * @return
     */
    public static <K, V> LinkedHashMap<K, V> linkedHashMap(int initialCapacity, float loadFactor) {
        return new LinkedHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new ConcurrentHashMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap() {
        return new ConcurrentHashMap<K, V>();
    }

    /**
     * Create a new ConcurrentHashMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap(int initialCapacity) {
        return new ConcurrentHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new ConcurrentHashMap.
     *
     * @param initialCapacity
     * @param loadFactor
     * @return
     */
    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap(int initialCapacity, float loadFactor) {
        return new ConcurrentHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new CaseInsensitiveMap.
     *
     * @param <K>             the key type
     * @param <V>             the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> CaseInsensitiveMap<V> caseInsensitiveMap() {
        return new CaseInsensitiveMap<V>();
    }

    /**
     * Create a new HashSet.
     *
     * @param initialCapacity
     * @return
     */
    public static <T> HashSet<T> hashSet(int initialCapacity) {
        return new HashSet<T>(initialCapacity);
    }

    /**
     * Create a new HashSet.
     *
     * @param initialCapacity
     * @param loadFactor
     * @return
     */
    public static <T> HashSet<T> hashSet(int initialCapacity, float loadFactor) {
        return new HashSet<T>(initialCapacity, loadFactor);
    }

    /**
     * Create a new LinkedHashSet.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> HashSet<T> linkedHashSet() {
        return new LinkedHashSet<T>();
    }


    /**
     * Create a new CopyOnWriteArraySet.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> CopyOnWriteArraySet<T> copyOnWriteArraySet() {
        return new CopyOnWriteArraySet<T>();
    }
    
    
    /**
     * Create a new CopyOnWriteArraySet.
     *
     * @param <T> the type
     * @return the object
     */
    public static ThreadFactory customThreadFactory(String namePrefix) {
        return new CustomThreadFactory(namePrefix);
    }
    
    
    
    private static final class CustomThreadFactory implements ThreadFactory {
        private final String prefix;
        private final boolean daemon;
        private final ThreadGroup group;
        private final AtomicInteger index = new AtomicInteger(1);

        public CustomThreadFactory(String prefix) {
            this(prefix, false);
        }
        
        private CustomThreadFactory(String prefix, boolean daemon) {
            SecurityManager sm = System.getSecurityManager();
            group = (sm != null) ? sm.getThreadGroup()
                    : Thread.currentThread().getThreadGroup();
            this.prefix = prefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            String name = prefix + "_" + index.getAndIncrement();
            Thread t = new Thread(group, r, name);
            t.setDaemon(daemon);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

}
