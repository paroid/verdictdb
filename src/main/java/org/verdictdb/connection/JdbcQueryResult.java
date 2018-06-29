package org.verdictdb.connection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcQueryResult implements DbmsQueryResult {
  
  List<String> columnNames = new ArrayList<>();

  List<Integer> columnTypes = new ArrayList<>();

  List<Boolean> isCurrency = new ArrayList<>();

  List<Integer> isNullable = new ArrayList<>();

  List<Integer> precision = new ArrayList<>();

  List<Integer> scale = new ArrayList<>();

  List<Integer> columnDisplaySize = new ArrayList<>();

  //ResultSet resultSet;

  List<List<Object>> result = new ArrayList<>();

  int cursor = -1;

  public JdbcQueryResult(ResultSet resultSet) throws SQLException {
    //this.resultSet = resultSet;
    ResultSetMetaData meta = resultSet.getMetaData();
    int columnCount = meta.getColumnCount();
    for (int i = 0; i < columnCount; i++) {
      columnNames.add(meta.getColumnLabel(i+1).toLowerCase());
      columnTypes.add(meta.getColumnType(i+1));
      isCurrency.add(meta.isCurrency(i+1));
      isNullable.add(meta.isNullable(i+1));
      precision.add(meta.getPrecision(i+1));
      scale.add(meta.getScale(i+1));
    }
    while (resultSet.next()) {
      List<Object> row = new ArrayList<>();
      for (int i=0; i< columnCount; i++) {
        row.add(resultSet.getObject(i+1));
      }
      result.add(row);
    }
  }

  @Override
  public int getColumnCount() {
    return columnNames.size();
  }

  @Override
  public String getColumnName(int index) {
    return columnNames.get(index);
  }

  @Override
  public int getColumnType(int index) {
    return columnTypes.get(index);
  }

  @Override
  public boolean next() {
    if (cursor<result.size()-1) {
      cursor++;
      return true;
    }
    else return false;
    /*
    boolean nextExists = false;
    try {
      nextExists = resultSet.next();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return nextExists;
    */
  }

  @Override
  public Object getValue(int index) {
    Object value = null;
    try {
      value = (Object) result.get(cursor).get(index);
      // value = resultSet.getObject(index + 1);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return value;
  }

  @Override
  public void printContent() {
    StringBuilder row;
    boolean isFirstCol = true;
    
    // print column names
    row = new StringBuilder();
    for (String col : columnNames) {
      if (isFirstCol) {
        row.append(col);
        isFirstCol = false;
      }
      else {
        row.append("\t" + col);
      }
    }
    System.out.println(row.toString());
    
    // print contents
    int colCount = getColumnCount();
    while(this.next()) {
      row = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        if (i == 0) {
          row.append(getValue(i).toString());
        }
        else {
          row.append("\t");
          row.append(getValue(i).toString());
        }
      }
      System.out.println(row.toString());
    }
    
  }

  public List<List<Object>> getResult() {
    return result;
  }

  public List<Boolean> getIsCurrency() {
    return isCurrency;
  }

  public List<Integer> getIsNullable() {
    return isNullable;
  }

  public List<Integer> getPrecision() {
    return precision;
  }

  public List<Integer> getScale() {
    return scale;
  }

  public List<Integer> getColumnDisplaySize() {
    return columnDisplaySize;
  }
}
