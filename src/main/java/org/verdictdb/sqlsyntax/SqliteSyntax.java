/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.sqlsyntax;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

public class SqliteSyntax extends SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    return false;
  }

  @Override
  public void dropTable(String schema, String tablename) { }

  @Override
  public String getFallbackDefaultSchema() {
    return "main";
  }

  @Override
  public int getColumnNameColumnIndex() {
    return 1;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "PRAGMA table_info(" + quoteName(table) + ")";
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 2;
  }

  @Override
  public String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getQuoteString() {
    return "'";
  }

  @Override
  public String getSchemaCommand() {
    // Sqlite doesn't support schema, use database instead
    return "PRAGMA database_list";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 1;
  }

  @Override
  public String getTableCommand(String schema) {
    // schema should be database in sqlite
    return String.format("select name from %s.sqlite_master where type='table'", schema);
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public String randFunction() {
    return "Abs(random()) % 100.0 / 100.0";
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }

  @Override
  public String hashFunction(String column) {
    return null;
  }

  public String createDatabase(String database) {
    try {
      String url = "jdbc:sqlite:" + database + ".db";
      Connection connection = DriverManager.getConnection(url);
      ResultSet rs = connection.createStatement().executeQuery("PRAGMA database_list");
      rs.next();
      String file = rs.getString(3);
      return "ATTACH " + quoteName(file) + " AS " + database;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return null;
  }
}
