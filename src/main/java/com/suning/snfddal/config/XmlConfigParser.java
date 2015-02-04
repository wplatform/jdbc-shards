package com.suning.snfddal.config;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.suning.snfddal.config.Configuration.DataSourceConfig;
import com.suning.snfddal.config.Configuration.RuleAlgorithmConfig;
import com.suning.snfddal.config.Configuration.SchemaConfig;
import com.suning.snfddal.config.Configuration.ShardConfig;
import com.suning.snfddal.config.Configuration.TableConfig;
import com.suning.snfddal.config.parser.ConfigEntityResolver;
import com.suning.snfddal.config.parser.ParsingException;
import com.suning.snfddal.config.parser.XNode;
import com.suning.snfddal.config.parser.XPathParser;
import com.suning.snfddal.route.rule.TableRouter;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StringUtils;
import com.suning.snfddal.util.Utils;

public class XmlConfigParser {

    private XPathParser parser;

    private Configuration configuration;

    public XmlConfigParser(InputStream inputStream) {
        this(new XPathParser(inputStream, true, new ConfigEntityResolver()));
    }

    public XmlConfigParser(XPathParser parser) {
        this.configuration = Configuration.getInstance();
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
        parseRuleAlgorithms(xNode.evalNodes("/ddal-config/ruleAlgorithms/ruleAlgorithm"));
        parseSchemaConfig(xNode.evalNode("/ddal-config/schema"));
    }

    private void parseSettings(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            String proName = xNode.getStringAttribute("name");
            String proValue = xNode.getStringAttribute("value");
            if (StringUtils.isNullOrEmpty(proName)) {
                throw new ParsingException("Error parsing ddal-config XML . Cause: propery's name required.");
            }
            if (StringUtils.isNullOrEmpty(proValue)) {
                throw new ParsingException("Error parsing ddal-config XML . Cause: propery's value required.");
            }
            configuration.addSettings(proName, proValue);
        }
    }

    private void parseShards(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            ShardConfig shardConfig = new ShardConfig();
            String name = xNode.getStringAttribute("name");
            Properties properties = xNode.getChildrenAsProperties();
            String description = (String) properties.get("description");
            if (StringUtils.isNullOrEmpty(name)) {
                throw new ParsingException("Error parsing ddal-config XML . Cause: group's name required.");
            }
            if (StringUtils.isNullOrEmpty(description)) {
                throw new ParsingException("Error parsing ddal-config XML . Cause: group's groupDescription required.");
            }
            shardConfig.setName(name);
            shardConfig.setDescription(description);
            shardConfig.setProperties(properties);
            configuration.addShard(name, shardConfig);
        }
    }

    private void parseDataSource(List<XNode> xNodes) {
        for (XNode dataSourceNode : xNodes) {
            DataSourceConfig dsConfig = new DataSourceConfig();
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
                configuration.addDataNode(id, dataSource);
            } else {
                throw new ParsingException("Error parsing ddal-config XML . Cause: datasource attribute 'id' required.");
            }
        }
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

    private void parseRuleAlgorithms(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            RuleAlgorithmConfig config = new RuleAlgorithmConfig();
            String name = xNode.getStringAttribute("name");
            String clazz = xNode.getStringAttribute("class");
            if (StringUtils.isNullOrEmpty(name)) {
                throw new ParsingException("ruleAlgorithm attribute 'name' is required.");
            }
            if (StringUtils.isNullOrEmpty(clazz)) {
                throw new ParsingException("ruleAlgorithm attribute 'clazz' is required.");
            }
            config.setName(name);
            config.setClazz(clazz);
            config.setProperties(xNode.getChildrenAsProperties());
            Object ruleAlgorithm = constructAlgorithm(config);
            configuration.addRuleAlgorithm(name, ruleAlgorithm);
        }
    }

    private Object constructAlgorithm(RuleAlgorithmConfig algorithmConfig) {
        String clazz = algorithmConfig.getClazz();
        Properties properties = algorithmConfig.getProperties();
        BeanInfo beanInfo = null;
        try {
            Object ruleAlgorithm = Class.forName(clazz).newInstance();
            beanInfo = Introspector.getBeanInfo(ruleAlgorithm.getClass());
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                String propertyValue = properties.getProperty(propertyDescriptor.getName());
                if (propertyValue != null) {
                    setPropertyWithAutomaticType(ruleAlgorithm, propertyDescriptor, propertyValue);
                }
            }
            return beanInfo;
        } catch (InvocationTargetException e) {
            throw new ParsingException("There was an error to construct RuleAlgorithm " + clazz + " Cause: "
                    + e.getTargetException(), e);
        } catch (Exception e) {
            throw new ParsingException("There was an error to construct RuleAlgorithm " + clazz + " Cause: " + e, e);
        }
    }
    
    private void parseSchemaConfig(XNode xNode) {
        SchemaConfig dsConfig = new SchemaConfig();
        String name = xNode.getStringAttribute("name");
        String metadata = xNode.getStringAttribute("metadata");
        if (StringUtils.isNullOrEmpty(name)) {
            throw new ParsingException("schema attribute 'name' is required.");
        }
        dsConfig.setName(name);
        dsConfig.setMetadata(metadata);
        List<TableConfig> tableConfings = New.arrayList();
        List<XNode> xNodes = xNode.evalNodes("table");
        for (XNode tableNode : xNodes) {
            TableConfig config = new TableConfig();
            String tableName = tableNode.getStringAttribute("name");
            String tableMetadata = tableNode.getStringAttribute("metadata");
            String router = tableNode.getStringAttribute("router");
            if (StringUtils.isNullOrEmpty(tableName)) {
                throw new ParsingException("table attribute 'name' is required.");
            }
            config.setName(tableName);
            if (StringUtils.isNullOrEmpty(tableMetadata)) {
                config.setMetadata(dsConfig.getMetadata());
            } else {
                config.setMetadata(tableMetadata);
            }
            if (!StringUtils.isNullOrEmpty(router)) {
               TableRouter tableRouter = configuration.getTableRouters().get(router);
               if(tableRouter == null) {
                   throw new ParsingException("The table router '" + router + "' is not found.");
               }
               TableRouter copy = new TableRouter();
               copy.setId(tableRouter.getId());
               copy.setPartition(tableRouter.getPartition());
               copy.setShardRuleExpression(tableRouter.getShardRuleExpression());
               copy.setTableRuleExpression(tableRouter.getTableRuleExpression());
               copy.initTopology(config.getName());
               config.setTableRouter(copy);
            }
            config.setSchemaConfig(dsConfig);
            if(tableConfings.contains(config)) {
                throw new ParsingException("Duplicate table name '" + tableName + "' in schema.");
            }
            tableConfings.add(config);
        }
        dsConfig.setTables(tableConfings);
        configuration.setSchemaConfig(dsConfig);
    
    }

    private DataSource constructDataSource(DataSourceConfig dataSourceConfig) {
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
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (IllegalAccessException e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (ClassNotFoundException e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (Exception e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
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
    private void setPropertyWithAutomaticType(Object ruleAlgorithm, PropertyDescriptor pd, String propertyValue)
            throws IllegalAccessException, InvocationTargetException {
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
            pd.getWriteMethod().invoke(ruleAlgorithm, Character.valueOf(propertyValue.toCharArray()[1]));
        } else if (pType == String.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, propertyValue);
        } else {
            throw new IllegalArgumentException("Can't set " + ruleAlgorithm.getClass().getName() + "'s property "
                    + pd.getName() + ",the type is " + pType.getName());
        }
    }

}
