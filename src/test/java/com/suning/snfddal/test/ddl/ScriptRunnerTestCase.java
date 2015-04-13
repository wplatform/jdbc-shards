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
// Created on 2015年4月7日
// $Id$

package com.suning.snfddal.test.ddl;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;

import junit.framework.Assert;

import org.junit.Test;

import com.suning.snfddal.test.BaseSampleCase;
import com.suning.snfddal.util.ScriptRunner;
import com.suning.snfddal.util.Utils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ScriptRunnerTestCase extends BaseSampleCase {

    
    @Test
    public void runCreateScript() throws Exception {
      Connection conn = dataSource.getConnection();
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
