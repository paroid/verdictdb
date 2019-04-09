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

package org.verdictdb.sqlwriter;

import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqliteSyntax;

public class CreateSchemaToSql {

  protected SqlSyntax syntax;

  public CreateSchemaToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(CreateSchemaQuery query) {
    if (syntax instanceof SqliteSyntax) {
      return ((SqliteSyntax) syntax).createDatabase(query.getSchemaName());
    }

    StringBuilder sql = new StringBuilder();
    sql.append("create schema ");
    if (query.isIfNotExists()) {
      sql.append("if not exists ");
    }
    sql.append(query.getSchemaName());
    return sql.toString();
  }
}
