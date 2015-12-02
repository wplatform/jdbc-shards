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

package com.wplatform.ddal.dispatch.rule;

import java.util.List;
import java.util.Map;

import com.wplatform.ddal.config.Configuration;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class OgnlRuleEvaluator implements RuleEvaluator {

    @Override
    public Object evaluate(RuleExpression expression, Map<String, Value> parameters) throws RuleEvaluateException {
        Map<String, Object> evaluateContext = New.hashMap();
        String ognlExpr = expression.getExpression();
        try {
            List<RuleColumn> ruleCols = expression.getRuleColumns();
            Map<String, Object> args = New.hashMap(ruleCols.size(), 1L);
            for (RuleColumn ruleColumn : ruleCols) {
                Value argValue = parameters.get(ruleColumn.getName());
                if (argValue == null) {
                    args.put(ruleColumn.getName(), null);
                } else {
                    args.put(ruleColumn.getName(), argValue.getObject());
                }
            }
            Configuration configuration = expression.getTableRouter().getConfiguration();
            Map<String, Object> algorithms = configuration.getRuleAlgorithms();
            evaluateContext.putAll(algorithms);
            evaluateContext.putAll(args);
            Object result = OgnlCache.getValue(ognlExpr, evaluateContext);
            if (result == null) {
                throw new RuleEvaluateException("The rule expression " + ognlExpr
                        + " return a null value.");
            }
            return result;
        } catch (RuleEvaluateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuleEvaluateException("Evaluate rule " + ognlExpr + "error, parameter is " + evaluateContext, e);
        }


    }

}
