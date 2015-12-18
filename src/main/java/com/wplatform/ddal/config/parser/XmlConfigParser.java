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
package com.wplatform.ddal.config.parser;

import com.wplatform.ddal.config.*;
import com.wplatform.ddal.config.ShardConfig.ShardItem;
import com.wplatform.ddal.route.algorithm.Partitioner;
import com.wplatform.ddal.route.rule.TableNode;
import com.wplatform.ddal.route.rule.TableRouter;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.util.Utils;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class XmlConfigParser {

    private XPathParser parser;

    private Configuration configuration;

    public XmlConfigParser(InputStream inputStream) {
        this(new XPathParser(inputStream, true, new ConfigEntityResolver()));
    }

    public XmlConfigParser(XPathParser parser) {
        this.configuration = new Configuration();
        this.parser = parser;
    }

    public Configuration parse() {
        try {
            parseElement(parser.evalNode("/ddal-config"));
            return configuration;
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException("Error parsing ddal-config XML . Cause: " + e, e);
        }
    }

    private void parseElement(XNode xNode) {
        parseSettings(xNode.evalNodes("/ddal-config/settings/property"));
        parseShards(xNode.evalNodes("/ddal-config/cluster/shard"));
        parseDataSource(xNode.evalNodes("/ddal-config/dataNodes/datasource"));
        parseRuleConfig(xNode.evalNodes("/ddal-config/tableRules/tableRule"));
        parseSchemaConfig(xNode.evalNode("/ddal-config/schema"));
    }

    private void parseSettings(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            String proName = xNode.getStringAttribute("name");
            String proValue = xNode.getStringAttribute("value");
            if (StringUtils.isNullOrEmpty(proName)) {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: propery's name required.");
            }
            if (StringUtils.isNullOrEmpty(proValue)) {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: propery's value required.");
            }
            configuration.setProperty(proName, proValue);
        }
    }

    private void parseShards(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            ShardConfig shardConfig = new ShardConfig();
            String name = xNode.getStringAttribute("name");
            if (StringUtils.isNullOrEmpty(name)) {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: element cluster.shard's name required.");
            }
            shardConfig.setName(name);
            List<XNode> children = xNode.evalNodes("member");
            List<ShardItem> shardItems = New.arrayList(children.size());
            for (XNode child : children) {
                ShardItem shardItem = new ShardItem();
                String ref = child.getStringAttribute("ref");
                int wWeight, rWeight;
                try {
                    wWeight = child.getIntAttribute("wWeight", 1);
                    rWeight = child.getIntAttribute("rWeight", 1);
                } catch (Exception e) {
                    throw new ParsingException("incorrect wWeight or rWeight 'value for member");
                }
                if (StringUtils.isNullOrEmpty(ref)) {
                    throw new ParsingException("member 's ref is required.");
                }
                if (wWeight <= 0 && rWeight <= 0) {
                    throw new ParsingException("member 's weight not be less than zero.");
                }
                if (wWeight <= 0) {
                    shardItem.setReadOnly(true);
                }
                shardItem.setRef(ref);
                shardItem.setwWeight(wWeight);
                shardItem.setrWeight(rWeight);
                if (shardItems.contains(shardItem)) {
                    throw new ParsingException("Duplicate datasource reference in " + name);
                }
                shardItems.add(shardItem);
            }
            shardConfig.setShardItems(shardItems);
            configuration.addShard(name, shardConfig);
        }
    }

    private void parseDataSource(List<XNode> xNodes) {
        XmlDataSourceProvider provider = new XmlDataSourceProvider();
        for (XNode dataSourceNode : xNodes) {
            DataNodeConfig dsConfig = new DataNodeConfig();
            String id = dataSourceNode.getStringAttribute("id");
            String jndiName = dataSourceNode.getStringAttribute("jndi-name");
            String clazz = dataSourceNode.getStringAttribute("class");
            if (!StringUtils.isNullOrEmpty(id)) {
                dsConfig.setId(id);
                if (!StringUtils.isNullOrEmpty(jndiName)) {
                    dsConfig.setJndiName(jndiName);
                } else if (!StringUtils.isNullOrEmpty(clazz)) {
                    dsConfig.setClazz(clazz);
                } else {
                    throw new ParsingException("datasource must be 'jndi-name' 'class' type.");
                }
                dsConfig.setProperties(dataSourceNode.getChildrenAsProperties());
                DataSource dataSource = constructDataSource(dsConfig);
                provider.addDataNode(id, dataSource);
            } else {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: datasource attribute 'id' required.");
            }
        }
        configuration.setObject("dataSourceProvider", provider);
    }

    private void parseRuleConfig(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            String resource = xNode.getStringAttribute("resource");
            if (StringUtils.isNullOrEmpty(resource)) {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: tableRule attribute 'resource' required.");
            }
            InputStream source = Utils.getResourceAsStream(resource);
            if (source == null) {
                throw new ParsingException("Can't load the table rule resource " + resource);
            }
            new XmlRuleConfigParser(source, configuration).parse();
        }

    }

    /**
     * @param tableConfigs
     * @param config
     */
    private void addToListIfNotDuplicate(List<TableConfig> tableConfigs, TableConfig config) {
        if (tableConfigs.contains(config)) {
            throw new ParsingException(
                    "Duplicate table name '" + config.getName() + "' in schema.");
        }
        tableConfigs.add(config);
    }

    private void parseSchemaConfig(XNode xNode) {
        SchemaConfig dsConfig = new SchemaConfig();
        String name = xNode.getStringAttribute("name");
        String shard = xNode.getStringAttribute("shard");
        if (StringUtils.isNullOrEmpty(name)) {
            throw new ParsingException("schema attribute 'name' is required.");
        }
        dsConfig.setName(name);
        dsConfig.setShard(shard);
        List<TableConfig> tableConfings = New.arrayList();
        List<XNode> xNodes = xNode.evalNodes("tableGroup");
        for (XNode tableGroupNode : xNodes) {
            Map<String, String> attributes = parseTableGroupAttributs(tableGroupNode);
            xNodes = tableGroupNode.evalNodes("table");
            for (XNode tableNode : xNodes) {
                TableConfig config = newTableConfig(dsConfig, attributes, tableNode);
                addToListIfNotDuplicate(tableConfings, config);
            }
        }
        xNodes = xNode.evalNodes("table");
        for (XNode tableNode : xNodes) {
            TableConfig config = newTableConfig(dsConfig, null, tableNode);
            addToListIfNotDuplicate(tableConfings, config);
        }
        dsConfig.setTables(tableConfings);
        configuration.setSchemaConfig(dsConfig);
        configuration.getTableRouterTemplates().clear();

    }

    /**
     * @param config
     * @param scanLevel
     */
    private void setTableScanLevel(TableConfig config, String scanLevel) {
        if ("unlimited".equals(scanLevel)) {
            config.setScanLevel(TableConfig.SCANLEVEL_UNLIMITED);
        } else if ("filter".equals(scanLevel)) {
            config.setScanLevel(TableConfig.SCANLEVEL_FILTER);
        } else if ("anyIndex".equals(scanLevel)) {
            config.setScanLevel(TableConfig.SCANLEVEL_ANYINDEX);
        } else if ("uniqueIndex".equals(scanLevel)) {
            config.setScanLevel(TableConfig.SCANLEVEL_UNIQUEINDEX);
        } else if ("shardingKey".equals(scanLevel)) {
            config.setScanLevel(TableConfig.SCANLEVEL_SHARDINGKEY);
        }
    }

    /**
     * @param tableNode
     * @return
     */
    private Map<String, String> parseTableGroupAttributs(XNode tableNode) {
        Map<String, String> attributes = New.hashMap();
        String shard = tableNode.getStringAttribute("shard");
        String router = tableNode.getStringAttribute("router");
        String validation = tableNode.getStringAttribute("validation");
        String scanLevel = tableNode.getStringAttribute("scanLevel");

        attributes.put("shard", shard);
        attributes.put("router", router);
        attributes.put("validation", validation);
        attributes.put("scanLevel", scanLevel);
        return attributes;
    }

    /**
     * @param scConfig
     * @param template
     * @param tableNode
     * @return
     */
    private TableConfig newTableConfig(SchemaConfig scConfig, Map<String, String> template,
                                       XNode tableNode) {
        TableConfig config = new TableConfig();
        String router = null;
        boolean validation = scConfig.isValidation();
        String scanLevel = "none";
        String shard = null;
        if (template != null) {
            router = template.get("router");
            String s = template.get("validation");
            if (!StringUtils.isNullOrEmpty(s)) {
                validation = Boolean.parseBoolean(s);
            }
            scanLevel = template.get("scanLevel");
            shard = template.get("shard");
        }
        String tableName = tableNode.getStringAttribute("name");

        String routerChild = tableNode.getStringAttribute("router", router);
        validation = tableNode.getBooleanAttribute("validation", validation);
        scanLevel = tableNode.getStringAttribute("scanLevel", scanLevel);
        shard = tableNode.getStringAttribute("shard", shard);
        if (StringUtils.isNullOrEmpty(shard)) {
            shard = scConfig.getShard();
        }
        if (StringUtils.isNullOrEmpty(tableName)) {
            throw new ParsingException("table attribute 'name' is required.");
        }
        if (!StringUtils.isNullOrEmpty(router) && !StringUtils.equals(router, routerChild)) {
            throw new ParsingException(
                    "table's attribute 'router' can't override tableGroup's attribute 'router'.");
        }
        if (StringUtils.isNullOrEmpty(router) && StringUtils.isNullOrEmpty(shard)) {
            throw new ParsingException(
                    "attribute 'shard' must be null if attribute 'router' was assigned.");
        }
        router = routerChild;
        config.setName(tableName);
        config.setValidation(validation);
        setTableScanLevel(config, scanLevel);


        List<String> nodes = New.arrayList();
        if (!StringUtils.isNullOrEmpty(shard)) {
            for (String string : shard.split(",")) {
                if (!string.trim().isEmpty() && !nodes.contains(string)) {
                    nodes.add(string);
                }
            }
            TableNode[] tableNodes = new TableNode[nodes.size()];
            for (int i = 0; i < nodes.size(); i++) {
                tableNodes[i] = new TableNode(nodes.get(i), tableName);
            }
            config.setShards(tableNodes);
        }

        if (!StringUtils.isNullOrEmpty(router)) {
            TableRouter rawRouter = configuration.getTableRouterTemplates().get(router);
            if (rawRouter == null) {
                throw new ParsingException("The table router '" + router + "' is not found.");
            }
            TableRouter tableRouter = new TableRouter();
            List<TableNode> partition = rawRouter.getPartition();
            List<TableNode> inited = New.arrayList(partition.size());
            for (TableNode item : partition) {
                String shardName = item.getShardName();
                String name = item.getObjectName();
                String suffix = item.getSuffix();
                if (StringUtils.isNullOrEmpty(name)) {
                    name = tableName;
                }
                TableNode initedNode = new TableNode(shardName, name, suffix);
                inited.add(initedNode);
            }

            tableRouter.setId(rawRouter.getId());
            tableRouter.setPartition(inited);
            String algorithm = rawRouter.getAlgorithm();
            tableRouter.setAlgorithm(algorithm);
            tableRouter.setRuleColumns(rawRouter.getRuleColumns());
            Partitioner partitioner = configuration.getPartitioner(algorithm);
            partitioner.initialize(inited);
            tableRouter.setPartitioner(partitioner);
            config.setTableRouter(tableRouter);
            config.setShards(null);
        }
        config.setSchemaConfig(scConfig);
        return config;
    }

    private DataSource constructDataSource(DataNodeConfig dataSourceConfig) {
        String id = dataSourceConfig.getId();
        DataSource dataSource;
        try {
            if (dataSourceConfig.isJndiDataSource()) {
                String jndiName = dataSourceConfig.getJndiName();
                dataSource = lookupJndiDataSource(jndiName, dataSourceConfig.getProperties());
            } else {
                String clazz = dataSourceConfig.getClazz();
                Properties properties = dataSourceConfig.getProperties();
                dataSource = (DataSource) Class.forName(clazz).newInstance();
                BeanInfo beanInfo = Introspector.getBeanInfo(dataSource.getClass());
                PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
                for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                    String propertyValue = properties.getProperty(propertyDescriptor.getName());
                    if (propertyValue != null) {
                        setPropertyWithAutomaticType(dataSource, propertyDescriptor, propertyValue);
                    }
                }
            }
        } catch (InstantiationException e) {
            throw new DataSourceException(
                    "There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (IllegalAccessException e) {
            throw new DataSourceException(
                    "There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (ClassNotFoundException e) {
            throw new DataSourceException(
                    "There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (Exception e) {
            throw new DataSourceException(
                    "There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        }
        return dataSource;

    }

    private DataSource lookupJndiDataSource(String jndiName, Properties jndiContext) {
        DataSource dataSource = null;
        try {
            InitialContext initCtx = null;
            initCtx = new InitialContext(jndiContext);
            dataSource = (DataSource) initCtx.lookup(jndiName);
            return dataSource;
        } catch (NamingException e) {
            throw new DataSourceException("There was an error configuring JndiDataSource.", e);
        } catch (Exception e) {
            throw new DataSourceException("There was an error configuring JndiDataSource.", e);
        }
    }

    /**
     * @param ruleAlgorithm
     * @param pd
     * @param propertyValue
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    static void setPropertyWithAutomaticType(Object ruleAlgorithm, PropertyDescriptor pd,
                                              String propertyValue) throws IllegalAccessException, InvocationTargetException {
        Class<?> pType = pd.getPropertyType();
        if (pType == Short.class || pType == short.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Short.valueOf(propertyValue));
        } else if (pType == Byte.class || pType == byte.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Byte.valueOf(propertyValue));
        } else if (pType == Integer.class || pType == int.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Integer.valueOf(propertyValue));
        } else if (pType == Double.class || pType == double.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Double.valueOf(propertyValue));
        } else if (pType == Float.class || pType == float.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Float.valueOf(propertyValue));
        } else if (pType == Boolean.class || pType == boolean.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Boolean.valueOf(propertyValue));
        } else if (pType == Long.class || pType == long.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Long.valueOf(propertyValue));
        } else if (pType == Character.class || pType == char.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm,
                    Character.valueOf(propertyValue.toCharArray()[1]));
        } else if (pType == String.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, propertyValue);
        } else {
            throw new IllegalArgumentException("Can't set " + ruleAlgorithm.getClass().getName()
                    + "'s property " + pd.getName() + ",the type is " + pType.getName());
        }
    }

}
