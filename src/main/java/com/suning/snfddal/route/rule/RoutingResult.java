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
// Created on 2015年1月14日
// $Id$

package com.suning.snfddal.route.rule;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<MatchedShard> matchedShards;

    /**
     * @return the matchedShards
     */
    public List<MatchedShard> getMatchedShards() {
        return matchedShards;
    }

    /**
     * @param matchedShards the matchedShards to set
     */
    public void setMatchedShards(List<MatchedShard> matchedShards) {
        this.matchedShards = matchedShards;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((matchedShards == null) ? 0 : matchedShards.hashCode());
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
        RoutingResult other = (RoutingResult) obj;
        if (matchedShards == null) {
            if (other.matchedShards != null)
                return false;
        } else if (!matchedShards.equals(other.matchedShards))
            return false;
        return true;
    }




    public static class MatchedShard implements Serializable {
        private static final long serialVersionUID = 1L;
        private String shardName;
        private String[] tables;

        /**
         * @return the shardName
         */
        public String getShardName() {
            return shardName;
        }

        /**
         * @param shardName the shardName to set
         */
        public void setShardName(String shardName) {
            this.shardName = shardName;
        }

        /**
         * @return the tables
         */
        public String[] getTables() {
            return tables;
        }

        /**
         * @param tables the tables to set
         */
        public void setTables(String[] tables) {
            this.tables = tables;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((shardName == null) ? 0 : shardName.hashCode());
            result = prime * result + Arrays.hashCode(tables);
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
            MatchedShard other = (MatchedShard) obj;
            if (shardName == null) {
                if (other.shardName != null)
                    return false;
            } else if (!shardName.equals(other.shardName))
                return false;
            if (!Arrays.equals(tables, other.tables))
                return false;
            return true;
        }

        
        
    }

}
