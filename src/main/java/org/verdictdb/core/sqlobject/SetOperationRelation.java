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

package org.verdictdb.core.sqlobject;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SetOperationRelation extends SelectQuery {

  private static final long serialVersionUID = -1691931967730375434L;

  // May need to expand
  public enum SetOpType {
    union,
    unionAll,
    except,
    intersect,
    unknown
  }

  AbstractRelation left, right;

  SetOpType setOpType;

  // The key is the table alias name and the value is the index of the query which contains that table.
  // For instance, query0 UNION query1 and query1 contain table vt1, then ("vt1", 0) is recorded.
  HashMap<AbstractRelation, Integer> tableQueryIndexMap = new HashMap<>();

  private List<SelectQuery> selectQueryList;

  public SetOperationRelation(AbstractRelation left, AbstractRelation right, SetOpType setOpType) {
    this.left = left;
    this.right = right;
    this.setOpType = setOpType;

    // Set up from list
    List<SelectQuery> selectQueryList = getAllSelectQueryInSetOperation();
    for (SelectQuery q:selectQueryList) {
      for (AbstractRelation relation:q.fromList) {
        tableQueryIndexMap.put(relation, selectQueryList.indexOf(q));
      }
    }
    this.selectQueryList = selectQueryList;
  }

  public AbstractRelation getLeft() {
    return left;
  }

  public AbstractRelation getRight() {
    return right;
  }

  public String getSetOpType() {
    if (setOpType.equals(SetOpType.union)) {
      return "UNION";
    } else if (setOpType.equals(SetOpType.unionAll)) {
      return "UNION ALL";
    } else if (setOpType.equals(SetOpType.except)) {
      return "EXCEPT";
    } else if (setOpType.equals(SetOpType.intersect)) {
      return "INTERSECT";
    } else return "UNION";
  }

  private List<SelectQuery> getAllSelectQueryInSetOperation() {
    List<SelectQuery> selectQueryList = new ArrayList<>();
    if (!(this.getLeft() instanceof SetOperationRelation)) {
      selectQueryList.add((SelectQuery) this.getLeft());
    } else {
      selectQueryList.addAll(((SetOperationRelation)this.getLeft()).getAllSelectQueryInSetOperation());
    }
    if (!(this.getRight() instanceof SetOperationRelation)) {
      selectQueryList.add((SelectQuery) this.getRight());
    } else {
      selectQueryList.addAll(((SetOperationRelation)this.getRight()).getAllSelectQueryInSetOperation());
    }
    return selectQueryList;
  }

  public List<SelectQuery> getSelectQueryList() {
    return selectQueryList;
  }

  @Override
  public List<AbstractRelation> getFromList() {
    return fromList;
  }

  @Override
  public SelectQuery deepcopy() {
    return new SetOperationRelation(left.deepcopy(), right.deepcopy(), setOpType);
  }

  // This is necessary to insert verdictdbblock predicates for scramble tables.
  @Override
  public void addFilterByAnd(UnnamedColumn predicate) {
    try {
      if (predicate instanceof ColumnOp) {
        ColumnOp col = (ColumnOp) predicate;
        BaseColumn baseColumn = (BaseColumn) col.getOperand(0);
        String tableAlias = baseColumn.getTableSourceAlias();
        int idx = getQueryIndex(tableAlias);
        selectQueryList.get(idx).addFilterByAnd(predicate);
      }
    } catch (ClassCastException e) {
      throw e;
    }
  }

  public int getQueryIndex(String tableAliasName) {
    for (AbstractRelation from:fromList) {
      if (from.getAliasName().isPresent() &&
          tableAliasName.equals(from.getAliasName().get())) {
        return tableQueryIndexMap.get(from);
      }
    }
    return -1;
  }

/**
  // if the set operation relation contains scramble table, we only scramble the subquery with most scramble tables to
  // avoid the duplicate of scramble nodes
  public void removeDupFromScrambleNodes(
      List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodeAndTableName) {
    HashMap<Integer, List<Pair<ExecutableNodeBase, Triple<String, String, String>>>> scrambledNodeAndTableNameMap =
        new HashMap<>();
    for (Pair<ExecutableNodeBase, Triple<String, String, String>> scramble:scrambledNodeAndTableName) {
      String tableAlias = scramble.getRight().getRight();
      int idx = getQueryIndex(tableAlias);
      if (idx!=-1) {
        if (!scrambledNodeAndTableNameMap.containsKey(idx)) {
          scrambledNodeAndTableNameMap.put(idx, new ArrayList<>());
        }
        scrambledNodeAndTableNameMap.get(idx).add(scramble);
      }
    }

    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambleNodes = new ArrayList<>();
    for (Map.Entry<Integer, List<Pair<ExecutableNodeBase, Triple<String, String, String>>>>
        entry:scrambledNodeAndTableNameMap.entrySet()) {
      if (entry.getValue().size()>scrambleNodes.size()) {
        scrambleNodes = entry.getValue();
      }
      scrambledNodeAndTableName.removeAll(entry.getValue());
    }
    scrambledNodeAndTableName.addAll(scrambleNodes);
  }
 **/
}
