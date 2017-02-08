/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.hbase;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Filters implements Runnable {
  private static final String TABLE_NAME = "JoshHBaseFiltersTest";
  private static final byte[] FAM1 = Bytes.toBytes("f1");
  private static final byte[] FAM2 = Bytes.toBytes("f2");

  public Filters() {}

  @Override
  public void run() {
    try {
      _run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void _run() throws Exception {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path("/usr/local/lib/hbase/conf/hbase-site.xml"));
    Connection conn = ConnectionFactory.createConnection(conf);

    TableName tn = dropAndCreateTable(conn);

    // Should see rows1 and 3
    writeData(conn, tn ,"row1", "YES", "YES");
    writeData(conn, tn ,"row2", "NO", "NO");
    writeData(conn, tn ,"row3", "YES", "YES");
    writeData(conn, tn ,"row4", "YES", "NO");
    writeData(conn, tn ,"row5", null, "YES");
    writeData(conn, tn ,"row6", "YES", null);

    try (Table t = conn.getTable(tn)) {
      ResultScanner rs = t.getScanner(new Scan());
      System.out.println("*** Reading results");
      for (Result result : rs) {
        System.out.println(result.toString());
      }
      System.out.println("*** Done reading results");
    }

    Set<String> rowsSeen = new HashSet<>();
    Scan scan = createScan();
    try (Table t = conn.getTable(tn)) {
      ResultScanner rs = t.getScanner(scan);
      System.out.println("*** Reading filtered results");
      for (Result result : rs) {
        rowsSeen.add(new String(result.getRow(), StandardCharsets.UTF_8));
        System.out.println(result.toString());
      }
      System.out.println("*** Done reading filtered results");
    }
    if (rowsSeen.size() != 2 || !rowsSeen.contains("row1") | !rowsSeen.contains("row3")) {
      throw new RuntimeException("Unexpected results. Saw rows: " + rowsSeen + ", should have seen {row1, row3}");
    }
    System.out.println("Success");
  }

  TableName dropAndCreateTable(Connection conn) throws Exception {
    Admin admin = conn.getAdmin();
    TableName tn = TableName.valueOf(TABLE_NAME);
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }
    HTableDescriptor htd = new HTableDescriptor(tn);
    htd.addFamily(new HColumnDescriptor(FAM1));
    htd.addFamily(new HColumnDescriptor(FAM2));
    admin.createTable(htd);
    return tn;
  }

  void writeData(Connection conn, TableName tn, String row, String val1, String val2) throws Exception {
    Put p = new Put(Bytes.toBytes(row));
    p.addColumn(FAM1, Bytes.toBytes("col1"), Bytes.toBytes("whatever"));
    if (null != val1) {
      p.addColumn(FAM2, Bytes.toBytes("col1"), Bytes.toBytes(val1));
    }
    if (null != val2) {
      p.addColumn(FAM2, Bytes.toBytes("col2"), Bytes.toBytes(val2));
    }
    Table t = conn.getTable(tn);
    t.put(p);
    t.close();
  }

  Scan createScan() {
    Scan scan = new Scan();
    FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    SingleColumnValueFilter cvFilter1 = new SingleColumnValueFilter(FAM2, Bytes.toBytes("col1"), CompareOp.EQUAL, Bytes.toBytes("YES"));
    cvFilter1.setFilterIfMissing(true);
    SingleColumnValueFilter cvFilter2 = new SingleColumnValueFilter(FAM2, Bytes.toBytes("col2"), CompareOp.EQUAL, Bytes.toBytes("YES"));
    cvFilter2.setFilterIfMissing(true);
    list.addFilter(cvFilter1);
    list.addFilter(cvFilter2);
    scan.addColumn(FAM2, Bytes.toBytes("col1"));
    scan.addColumn(FAM2, Bytes.toBytes("col2"));
    scan.setFilter(list);
    return scan;
  }

  public static void main(String[] args) throws Exception {
    new Filters().run();
  }
}
