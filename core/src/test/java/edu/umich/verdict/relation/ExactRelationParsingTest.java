package edu.umich.verdict.relation;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.umich.verdict.VerdictContext;

public class ExactRelationParsingTest {

  @Test
  public void testSelectFlat() {
    VerdictContext vc = VerdictContext.dummyContext();
    String sql = "select mycolumn from mytable";
    Relation.resetAliasNumbers();
    ExactRelation r = ExactRelation.from(vc, sql);
    assertEquals("SELECT `mycolumn` AS `mycolumn` FROM `mytable` AS vt1", r.toSql());
  }
  
  @Test
  public void testSelectFlatQuotedColumn() {
    VerdictContext vc = VerdictContext.dummyContext();
    String sql = "select \"mycolumn\" from mytable";
    Relation.resetAliasNumbers();
    ExactRelation r = ExactRelation.from(vc, sql);
    assertEquals("SELECT `mycolumn` AS `mycolumn` FROM `mytable` AS vt1", r.toSql());
  }
  
  @Test
  public void testSelectFlatQuotedTable() {
    VerdictContext vc = VerdictContext.dummyContext();
    String sql = "select mycolumn from \"mytable\"";
    Relation.resetAliasNumbers();
    ExactRelation r = ExactRelation.from(vc, sql);
    assertEquals("SELECT `mycolumn` AS `mycolumn` FROM `mytable` AS vt1", r.toSql());
  }
  
  @Test
  public void testSelectFlatQuotedTable2() {
    VerdictContext vc = VerdictContext.dummyContext();
    String sql = "select mycolumn from \"myschema\".\"mytable\"";
    Relation.resetAliasNumbers();
    ExactRelation r = ExactRelation.from(vc, sql);
    assertEquals("SELECT `mycolumn` AS `mycolumn` FROM `myschema`.`mytable` AS vt1", r.toSql());
  }

}
