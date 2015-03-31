/*
 * Copyright 2014 suning.com Holding Ltd.
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
// Created on 2014年3月25日
// $Id$

package com.suning.snfddal.config.parser;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.suning.snfddal.config.Configuration;
import com.suning.snfddal.route.rule.RuleColumn;
import com.suning.snfddal.route.rule.RuleExpression;
import com.suning.snfddal.route.rule.TableRouter;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StringUtils;

public class XmlRuleConfigParser {

    private XPathParser parser;

    private Configuration configuration;

    public XmlRuleConfigParser(InputStream inputStream, Configuration configuration) {
        this(new XPathParser(inputStream, true, new RuleEntityResolver()), configuration);
    }

    public XmlRuleConfigParser(XPathParser parser, Configuration configuration) {
        this.configuration = configuration;
        this.parser = parser;
    }

    public void parse() {
        configurationElement(parser.evalNode("/ddal-rule"));
    }

    private void configurationElement(XNode context) {
        try {
            parseTableRule(context.evalNodes("/ddal-rule/tableRouter"));
        } catch (Exception e) {
            throw new ParsingException("Error parsing ddal-rule XML . Cause: " + e, e);
        }
    }

    private void parseTableRule(List<XNode> list) throws Exception {
        for (XNode xNode : list) {
            String id = xNode.getStringAttribute("id");
            if (StringUtils.isNullOrEmpty(id)) {
                throw new ParsingException(
                        "Error parsing ddal-rule XML . Cause: the id attribute of 'tableRouter' element is required.");
            }
            TableRouter tableRule = new TableRouter();
            tableRule.setId(id);
            parseTableRuleChildrenXNode(tableRule, xNode.getChildren());
            configuration.addTableRouter(id, tableRule);
        }
    }

    // 解析<tableRule>标签下的所有子标签
    private void parseTableRuleChildrenXNode(TableRouter tableRouter, List<XNode> list) {

        for (XNode xNode : list) {
            if ("partition".equals(xNode.getName())) {
                String partition = getStringBody(xNode);
                if (partition != null) {
                    partition = partition.replaceAll("\\s", "");
                }
                if (StringUtils.isNullOrEmpty(partition)) {
                    throw new ParsingException("RuleTable '" + tableRouter.getId() + "' partition is emptry.");
                }
                parsePartition(tableRouter, partition);
            } else if ("shardRule".equals(xNode.getName())) {
                RuleExpression ruleExpr = parseRuleExpression(xNode, tableRouter);
                tableRouter.setShardRuleExpression(ruleExpr);
            } else if ("tableRule".equals(xNode.getName())) {
                RuleExpression ruleExpr = parseRuleExpression(xNode, tableRouter);
                tableRouter.setTableRuleExpression(ruleExpr);
            }
        }

    }
    
    private String getStringBody(XNode xNode) {
        StringBuilder sb = new StringBuilder();
        NodeList children = xNode.getNode().getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            XNode child = xNode.newXNode(children.item(i));
            String nodeName = child.getNode().getNodeName();
            if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE
                    || child.getNode().getNodeType() == Node.TEXT_NODE) {
                String data = child.getStringBody("");
                sb.append(data);
            } else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) {
                throw new ParsingException("Unknown element <" + nodeName + "> in rule XML .");
            }
        }
        String body = sb.toString();
        return body;
    }

    // 解析<rule>标签的内容
    public RuleExpression parseRuleExpression(XNode xNode, TableRouter tableRouter) {
        String stringBody = getStringBody(xNode);
        String text = stringBody.replaceAll("\\s", " ");
        final List<RuleColumn> ruleColumns = new ArrayList<RuleColumn>();
        GenericTokenParser parser = new GenericTokenParser("${", "}", new TokenHandler() {
            @Override
            public String handleToken(String content) {
                content = content.replaceAll("\\s", "");
                String name = null;
                String required = null;
                String type = null;
                if (content.contains(",")) {
                    String[] properties = content.split(",");
                    name = properties[0];
                    for (int j = 1; j < properties.length; j++) {
                        String propety = properties[j];
                        if (propety.contains("required=")) {
                            required = propety.split("required=")[1];
                        }
                        if (propety.contains("type=")) {
                            type = propety.split("type=")[1];
                        }
                    }
                } else {
                    name = content;
                }
                ruleColumns.add(newRuleColumn(name, required, type));
                return name;
            }
        });
        String expression = parser.parse(text);
        RuleExpression rule = new RuleExpression();
        rule.setExpression(expression);
        rule.setRuleColumns(ruleColumns);
        return rule;
    }

    // 封装成RuleColum
    public static RuleColumn newRuleColumn(String name, String required, String type) {
        RuleColumn ruleColumn = new RuleColumn();
        if (StringUtils.isNullOrEmpty(name) || name.contains("=")) {
            throw new ParsingException(
                    " Error parsing rule element in rule XML. Cause: the RuleColumn's name is required , and should "
                            + "in the first place .");
        } else {
            ruleColumn.setName(name);
        }
        if (!StringUtils.isNullOrEmpty(required)) {
            ruleColumn.setRequired(Boolean.valueOf(required));
        }
        if (!StringUtils.isNullOrEmpty(type)) {
            ruleColumn.setType(type.toLowerCase());
        }
        return ruleColumn;
    }

    private void parsePartition(TableRouter tableRouter, String partition) {
        Map<String, Set<String>> partitionConfig = New.linkedHashMap();
        String shards[] = partition.split(",");
        for (String shard : shards) {
            String shardName = null;
            int begin = shard.indexOf('[');
            int end = shard.lastIndexOf(']');
            String tableDesc = null;
            if (begin != -1 && end != -1) {
                tableDesc = shard.substring(begin + 1, end);
                shardName = shard.substring(0, begin);
            } else {
                shardName = shard;
            }
            Set<String> suffixs = New.linkedHashSet();
            if (tableDesc != null) {
                int length = tableDesc.length();
                ParsingException parsingException = new ParsingException(
                        "Error parsing partition element in rule XML. Cause： incorrect table suffix " + shard);
                if (length % 2 == 1 && tableDesc.charAt((length - 1) / 2) == '-') {
                    String connSign = parseConnSign(tableDesc.substring(0, length / 2));
                    String minNum = tableDesc.substring(0, length / 2).substring(connSign.length());
                    String maxNum = tableDesc.substring(length / 2 + 1).substring(connSign.length());
                    int min = Integer.valueOf(minNum);
                    int max = Integer.valueOf(maxNum);
                    if (min >= max) {
                        throw parsingException;
                    } else {
                        for (int j = min; j <= max; j++) {
                            String suffix = connSign + addZero(String.valueOf(j), minNum.length());
                            suffixs.add(suffix);
                        }
                    }
                } else {
                    throw parsingException;
                }
            }
            
            if (!configuration.getShardNames().contains(shardName)) {
                throw new ParsingException("Error parsing partition element in rule XML . "
                        + "Cause : The shard must exist in cluster .");
            } else if (partitionConfig.containsKey(shardName)) {
                throw new ParsingException(
                        "Error parsing partition element in rule XML . Cause : The shardName in TableRouter's "
                                + "partition must be unrepeatable .");
            }
            partitionConfig.put(shardName, suffixs);
        }
        tableRouter.setPartition(partitionConfig);
        
    }

    private String parseConnSign(String str) {
        String connSign = "";
        for (int i = 0; i < str.length(); i++) {
            if ('0' <= str.charAt(i) && str.charAt(i) <= '9') {
                connSign = str.substring(0, i);
                break;
            }
        }
        return connSign;
    }

    private String addZero(String str, int len) {
        int strLen = str.length();
        while (strLen < len) {
            str = "0" + str;
            strLen = str.length();
        }
        return str;
    }

}
