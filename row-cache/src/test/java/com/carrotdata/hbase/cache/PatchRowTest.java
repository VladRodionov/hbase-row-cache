/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.hbase.cache;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Test;

/**
 * The Class PatchRowTest.
 */
public class PatchRowTest extends BaseTest {

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(PatchRowTest.class);

  /**
   * Test key value patch.
   */
  @Test
  public void testKeyValuePatch() {
    LOG.info("KeyValue patch test started");
    KeyValue kv = new KeyValue("000000".getBytes(), "family".getBytes(), "column".getBytes(), 0,
        "value".getBytes());

    LOG.info("Old row=" + new String(TestUtils.getRow(kv)));
    patchRow(kv, "111".getBytes());
    LOG.info("New row=" + new String(TestUtils.getRow(kv)));

    assertEquals(new String("111000".getBytes()), new String(TestUtils.getRow(kv)));
    LOG.info("Finished OK");

  }

}
