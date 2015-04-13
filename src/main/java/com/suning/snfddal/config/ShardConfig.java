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
// Created on 2015年3月31日
// $Id$

package com.suning.snfddal.config;

import java.io.Serializable;
import java.util.List;

public class ShardConfig implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String name;
    private List<ShardItem> shardItems;
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the shardItems
     */
    public List<ShardItem> getShardItems() {
        return shardItems;
    }

    /**
     * @param shardItems the shardItems to set
     */
    public void setShardItems(List<ShardItem> shardItems) {
        this.shardItems = shardItems;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ShardConfig other = (ShardConfig) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }
    
    
    public static class ShardItem implements Serializable {        
        private static final long serialVersionUID = 1L;
        private boolean writable;
        private boolean readable;
        private int rWeight;
        private int wWeight;
        private String ref;
        /**
         * @return the writable
         */
        public boolean isWritable() {
            return writable;
        }
        /**
         * @param writable the writable to set
         */
        public void setWritable(boolean writable) {
            this.writable = writable;
        }
        /**
         * @return the readable
         */
        public boolean isReadable() {
            return readable;
        }
        /**
         * @param readable the readable to set
         */
        public void setReadable(boolean readable) {
            this.readable = readable;
        }
        /**
         * @return the rWeight
         */
        public int getrWeight() {
            return rWeight;
        }
        /**
         * @param rWeight the rWeight to set
         */
        public void setrWeight(int rWeight) {
            this.rWeight = rWeight;
        }
        /**
         * @return the wWeight
         */
        public int getwWeight() {
            return wWeight;
        }
        /**
         * @param wWeight the wWeight to set
         */
        public void setwWeight(int wWeight) {
            this.wWeight = wWeight;
        }
        /**
         * @return the ref
         */
        public String getRef() {
            return ref;
        }
        /**
         * @param ref the ref to set
         */
        public void setRef(String ref) {
            this.ref = ref;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((ref == null) ? 0 : ref.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ShardItem other = (ShardItem) obj;
            if (ref == null) {
                if (other.ref != null)
                    return false;
            } else if (!ref.equals(other.ref))
                return false;
            return true;
        }
        
        
    }

}