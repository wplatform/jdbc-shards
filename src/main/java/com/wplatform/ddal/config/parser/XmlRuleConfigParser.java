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
// Created on 2014年3月25日
// $Id$

package com.wplatform.ddal.config.parser;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.wplatform.ddal.config.Configuration;
import com.wplatform.ddal.dispatch.rule.RuleColumn;
import com.wplatform.ddal.dispatch.rule.RuleExpression;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.dispatch.rule.TableRouter;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
            //ruleColumn.setType(type.toLowerCase());
        }
        return ruleColumn;
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
            TableRouter routeConfig = new TableRouter(null);
            routeConfig.setId(id);
            parseTableRuleChildrenXNode(routeConfig, xNode.getChildren());
            configuration.addTemporaryTableRouter(id, routeConfig);
        }
    }

    // 解析<tableRule>标签下的所有子标签
    private void parseTableRuleChildrenXNode(TableRouter tableRouter, List<XNode> list) {

        for (XNode xNode : list) {
            if ("partition".equals(xNode.getName())) {
                parsePartition(tableRouter, xNode.getChildren());
            } else if ("tableRule".equals(xNode.getName())) {
                RuleExpression ruleExpr = parseRuleExpression(xNode);
                if (ruleExpr.getRuleColumns().isEmpty()) {
                    throw new ParsingException("The table router '" + tableRouter.getId()
                            + "' has no sharding column.");
                }
                tableRouter.setRuleExpression(ruleExpr);
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
    public RuleExpression parseRuleExpression(XNode xNode) {
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
        RuleExpression rule = new RuleExpression(null);
        rule.setExpression(expression);
        rule.setRuleColumns(ruleColumns);
        return rule;
    }

    private void parsePartition(TableRouter tableRouter, List<XNode> list) {
        List<TableNode> tableNodes = New.arrayList();
        for (XNode xNode : list) {
            String shard = xNode.getStringAttribute("shard");
            String suffix = xNode.getStringAttribute("suffix");
            shard = shard == null ? null : shard.trim();
            suffix = suffix == null ? null : suffix.trim();
            if (StringUtils.isNullOrEmpty(shard)) {
                throw new ParsingException("Error parsing ddal-rule XML. Cause: "
                        + "the shard attribute of 'table' element is required.");
            }
            List<String> shards = collectItems(shard);
            List<String> suffixes = collectItems(suffix);
            if (suffixes.isEmpty()) {
                for (String shardItem : shards) {
                    TableNode node = new TableNode(shardItem, null, null);
                    if (tableNodes.contains(node)) {
                        throw new ParsingException("Duplicate " + node + " defined in "
                                + tableRouter.getId() + "'s partition");
                    }
                    tableNodes.add(node);
                }
            } else {
                for (String shardItem : shards) {
                    for (String suffixItem : suffixes) {
                        TableNode node = new TableNode(shardItem, null, suffixItem);
                        if (tableNodes.contains(node)) {
                            throw new ParsingException("Duplicate " + node + " defined in "
                                    + tableRouter.getId() + "'s partition");
                        }
                        tableNodes.add(node);
                    }
                }
            }
        }
        tableRouter.setPartition(tableNodes);

    }

    /**
     * @param items
     * @param shards
     */
    private List<String> collectItems(String items) {
        List<String> result = New.arrayList();
        if (!StringUtils.isNullOrEmpty(items)) {
            for (String string : items.split(",")) {
                string = string.trim();
                if (StringUtils.isNullOrEmpty(string)) {
                    continue;
                }
                if (result.contains(string)) {
                    throw new ParsingException(
                            "Error parsing ddal-rule XML . Duplicate item '" + items + "'");
                }
                result.add(string);
            }
        }
        return result;
    }

}
