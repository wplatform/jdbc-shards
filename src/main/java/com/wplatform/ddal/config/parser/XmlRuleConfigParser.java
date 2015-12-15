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

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.wplatform.ddal.config.AlgorithmConfig;
import com.wplatform.ddal.config.Configuration;
import com.wplatform.ddal.route.algorithm.Partitioner;
import com.wplatform.ddal.route.rule.TableNode;
import com.wplatform.ddal.route.rule.TableRouter;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;

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
        try {
            parsePartitioner(parser.evalNodes("/ddal-rule/partitioner"));
            parseTableRouter(parser.evalNodes("/ddal-rule/tableRouter"));
        } catch (Exception e) {
            throw new ParsingException("Error parsing ddal-rule XML . Cause: " + e, e);
        }
    }


    private void parseTableRouter(List<XNode> list) throws Exception {
        for (XNode xNode : list) {
            String id = xNode.getStringAttribute("id");
            if (StringUtils.isNullOrEmpty(id)) {
                throw new ParsingException(
                        "Error parsing ddal-rule XML . Cause: the id attribute of 'tableRouter' element is required.");
            }
            TableRouter routeConfig = new TableRouter();
            routeConfig.setId(id);
            parseTableRouterProperties(routeConfig, xNode.getChildren());
            configuration.addTableRouterTemplate(id, routeConfig);
        }
    }

    private void parseTableRouterProperties(TableRouter tableRouter, List<XNode> list) {
        for (XNode xNode : list) {
            if ("partition".equals(xNode.getName())) {
                parsePartition(tableRouter, xNode.getChildren());
            } else if ("tableRule".equals(xNode.getName())) {
                for (XNode child : xNode.getChildren()) {
                    String name = child.getName();
                    String text = getStringBody(child);
                    text = text.replaceAll("\\s", "");
                    if("columns".equals(name)) {
                        if(StringUtils.isNullOrEmpty(text)) {
                            throw new ParsingException("The table router '" + tableRouter.getId()
                            + "' has no rules columns defined.");
                        }
                        List<String> columns = parseMoreText(text,",");
                        tableRouter.setRuleColumns(columns);
                    } else if("algorithm".equals(name)) {
                        if(StringUtils.isNullOrEmpty(text)) {
                            throw new ParsingException("The table router '" + tableRouter.getId()
                            + "' has no algorithm defined.");
                        }
                        tableRouter.setAlgorithm(text);
                    }
                }
            }
        }

    }

    /**
     * @param text
     * @return
     */
    private List<String> parseMoreText(String text,String split) {
        String[] strings = text.split(split);
        List<String> columns = New.arrayList(strings.length);
        for (String column : strings) {
            if(StringUtils.isNullOrEmpty(column)) {
                continue;
            }
            columns.add(column);
        }
        return columns;
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

    private void parsePartition(TableRouter tableRouter, List<XNode> list) {
        List<TableNode> tableNodes = New.arrayList();
        for (XNode xNode : list) {
            String shard = xNode.getStringAttribute("shard");
            String suffix = xNode.getStringAttribute("suffix");
            shard = shard == null ? null : shard.replaceAll("\\s", "");
            suffix = suffix == null ? null : suffix.replaceAll("\\s", "");
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
     */
    private List<String> collectItems(String items) {
        List<String> result = New.arrayList();
        if(StringUtils.isNullOrEmpty(items)) {
            return result;
        } else if (items.indexOf(",") != -1) {
            result = parseMoreText(items, ",");
            if (result.size() == new HashSet<String>(result).size()) {
                throw new ParsingException(
                        "Duplicate item " + items);
            }
        } else if (items.indexOf("-") != -1) {
            List<String> list = parseMoreText(items, "-");
            if (list.size() != 2) {
                throw new ParsingException(
                        "Invalid conjunction item'" + items + "'");
            }
        }
        return result;
    }
    
    private void parsePartitioner(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            AlgorithmConfig config = new AlgorithmConfig();
            String name = xNode.getStringAttribute("name");
            String clazz = xNode.getStringAttribute("class");
            if (StringUtils.isNullOrEmpty(name)) {
                throw new ParsingException("partitioner attribute 'name' is required.");
            }
            if (StringUtils.isNullOrEmpty(clazz)) {
                throw new ParsingException("partitioner attribute 'class' is required.");
            }
            config.setName(name);
            config.setClazz(clazz);
            config.setProperties(xNode.getChildrenAsProperties());
            Partitioner algorithm = constructPartitioner(config);
            configuration.addPartitioner(name, algorithm);
        }
    }
    
    private Partitioner constructPartitioner(AlgorithmConfig algorithmConfig) {
        String clazz = algorithmConfig.getClazz();
        Properties properties = algorithmConfig.getProperties();
        Partitioner partitioner = null;
        try {
            Object object = Class.forName(clazz).newInstance();
            Class<?> objectClass = object.getClass();
            if(!(object instanceof Partitioner)) {
                throw new ParsingException("invalid class " + objectClass.getName());
            }
            partitioner = (Partitioner) object;
            BeanInfo beanInfo = Introspector.getBeanInfo(objectClass);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                String propertyValue = properties.getProperty(propertyDescriptor.getName());
                if (propertyValue != null) {
                    XmlConfigParser.setPropertyWithAutomaticType(partitioner, propertyDescriptor, propertyValue);
                }
            }
            return partitioner;
        } catch (InvocationTargetException e) {
            throw new ParsingException("There was an error to construct partitioner " + clazz
                    + " Cause: " + e.getTargetException(), e);
        } catch (Exception e) {
            throw new ParsingException(
                    "There was an error to construct partitioner " + clazz + " Cause: " + e, e);
        }
    }

}
