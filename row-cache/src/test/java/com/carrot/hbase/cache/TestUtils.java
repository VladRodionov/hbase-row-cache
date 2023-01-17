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
package com.carrot.hbase.cache;

import org.apache.hadoop.hbase.Cell;

public class TestUtils {
  
  public static byte[] getRow(Cell cell) {
    byte[] array = cell.getRowArray();
    int off = cell.getRowOffset();
    int len = cell.getRowLength();
    byte[] buf = new byte[len];
    System.arraycopy(array, off, buf, 0, len);
    return buf;
  }
  
  public static byte[] getFamily(Cell cell) {
    byte[] array = cell.getFamilyArray();
    int off = cell.getFamilyOffset();
    int len = cell.getFamilyLength();
    byte[] buf = new byte[len];
    System.arraycopy(array, off, buf, 0, len);
    return buf;
  }
  
  public static byte[] getQualifier(Cell cell) {
    byte[] array = cell.getQualifierArray();
    int off = cell.getQualifierOffset();
    int len = cell.getQualifierLength();
    byte[] buf = new byte[len];
    System.arraycopy(array, off, buf, 0, len);
    return buf;
  }
}
