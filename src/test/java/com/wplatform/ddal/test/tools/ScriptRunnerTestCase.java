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
// Created on 2015年4月7日
// $Id$

package com.wplatform.ddal.test.tools;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;

import org.junit.Test;

import com.wplatform.ddal.test.BaseTestCase;
import com.wplatform.ddal.util.ScriptRunner;
import com.wplatform.ddal.util.Utils;

import junit.framework.Assert;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ScriptRunnerTestCase extends BaseTestCase {

    
    @Test
    public void runCreateScript() throws Exception {
      Connection conn = getConnection();
      ScriptRunner runner = new ScriptRunner(conn);
      runner.setAutoCommit(true);
      runner.setStopOnError(true);

      String resource = "script/mysql_script.sql";
      Reader reader = new InputStreamReader(Utils.getResourceAsStream(resource));

      try {
        runner.runScript(reader);
      } catch (Exception e) {
          Assert.fail(e.getMessage());
      }
    }
}
