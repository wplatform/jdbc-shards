package com.suning.snfddal.dispatch.rule;

import com.suning.snfddal.config.parser.ParsingException;
import ognl.*;

import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class OgnlCache {

    private static final Map<String, ognl.Node> expressionCache = new ConcurrentHashMap<String, ognl.Node>();

    public static Object getValue(String expression, Object root) {
        try {
            return Ognl.getValue(parseExpression(expression), root);
        } catch (OgnlException e) {
            throw new ParsingException("Error evaluating expression '" + expression + "'. Cause: " + e, e);
        }
    }

    private static Object parseExpression(String expression) throws OgnlException {
        try {
            Node node = expressionCache.get(expression);
            if (node == null) {
                node = new OgnlParser(new StringReader(expression)).topLevelExpression();
                expressionCache.put(expression, node);
            }
            return node;
        } catch (ParseException e) {
            throw new ExpressionSyntaxException(expression, e);
        } catch (TokenMgrError e) {
            throw new ExpressionSyntaxException(expression, e);
        }
    }

}
