package org.verdictdb.core.rewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.verdictdb.core.logical_query.SelectItem;
import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.AliasedColumn;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.ConstantColumn;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.logical_query.UnnamedColumn;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;

/**
 * AQP rewriter for partitioned tables. A sampling probability column must exist.
 * 
 * @author Yongjoo Park
 *
 */
public class ScrambleRewriter {
    
    ScrambleMeta scrambleMeta;
    
    int nextAliasNumber = 1;
    
    public ScrambleRewriter(ScrambleMeta scrambleMeta) {
        this.scrambleMeta = scrambleMeta;
    }
    
    String generateNextAliasName() {
        String aliasName = "verdictalias" + nextAliasNumber;
        nextAliasNumber += 1;
        return aliasName;
    }
    
    String generateMeanEstimateAliasName(String aliasName) {
        return aliasName;
    }
    
    String generateStdEstimateAliasName(String aliasName) {
        return "std_" + aliasName;
    }
    
    /**
     * Current Limitations:
     * 1. Only handles the query with a single aggregate (sub)query
     * 2. Only handles the query that the first select list is the aggregate query.
     * 
     * @param relation
     * @return
     * @throws VerdictDbException 
     */
    public List<AbstractRelation> rewrite(AbstractRelation relation) throws VerdictDbException {
//        int partitionCount = derivePartitionCount(relation);
        List<AbstractRelation> rewritten = new ArrayList<AbstractRelation>();
        
        AbstractRelation rewrittenWithoutPartition = rewriteNotIncludingMaterialization(relation);
        List<UnnamedColumn> partitionPredicates = generatePartitionPredicates(relation);
        
        for (int k = 0; k < partitionPredicates.size(); k++) {
            UnnamedColumn partitionPredicate = partitionPredicates.get(k);
            SelectQueryOp rewritten_k = deepcopySelectQuery((SelectQueryOp) rewrittenWithoutPartition);
            rewritten_k.addFilterByAnd(partitionPredicate);
            rewritten.add(rewritten_k);
        }
        
//        for (int k = 0; k < partitionCount; k++) {
//            rewritten.add(rewriteNotIncludingMaterialization(relation, k));
//        }
        
        return rewritten;
    }
    
    SelectQueryOp deepcopySelectQuery(SelectQueryOp relation) {
        SelectQueryOp sel = new SelectQueryOp();
        for (SelectItem c : relation.getSelectList()) {
            sel.addSelectItem(c);
        }
        for (AbstractRelation r : relation.getFromList()) {
            sel.addTableSource(r);
        }
        if (relation.getFilter().isPresent()) {
            sel.addFilterByAnd(relation.getFilter().get());
        }
        return sel;
    }
    
    /**
     * Rewrite a given query into AQP-enabled form. The rewritten queries do not include any "create table ..."
     * parts.
     * 
     * "select other_groups, sum(price) from ... group by other_groups;" is converted to
     * "select other_groups,
     *         sum(sub_sum_est) as mean_estimate,
     *         std(sub_sum_est*sqrt(subsample_size)) / sqrt(sum(subsample_size)) as std_estimate
     *  from (select other_groups,
     *               sum(price / prob) as sub_sum_est,
     *               count(*) as subsample_size
     *        from ...
     *        group by sid, other_groups) as sub_table
     *  group by other_groups;"
     *  
     * "select other_groups, avg(price) from ... group by other_groups;" is converted to
     * "select other_groups,
     *         sum(sub_sum_est) / sum(sub_count_est) as mean_estimate,
     *         std(sub_sum_est/sub_count_est*sqrt(subsample_size)) / sqrt(sum(subsample_size)) as std_estimate
     *  from (select other_groups,
     *               sum(price / prob) as sub_sum_est,
     *               sum(case 1 when price is not null else 0 end / prob) as sub_count_est,
     *               count(*) as subsample_size
     *        from ...
     *        group by sid, other_groups) as sub_table
     *  group by other_groups;"
     *  This is based on the self-normalized estimator.
     *  https://statweb.stanford.edu/~owen/mc/Ch-var-is.pdf
     *  
     * "select other_groups, count(*) from ... group by other_groups;" is converted to
     * "select other_groups,
     *         sum(sub_count_est) as mean_estimate
     *         sum(sub_count_est*sqrt(subsample_size)) / sqrt(sum(subsample_size)) as std_estimate,
     *  from (select other_groups,
     *               sum(1 / prob) as sub_count_est,
     *               count(*) as subsample_size
     *        from ...
     *        group by sid, other_groups) as sub_table
     *  group by other_groups;"
     * 
     * @param relation
     * @param partitionNumber
     * @return
     * @throws UnexpectedTypeException
     */
    AbstractRelation rewriteNotIncludingMaterialization(AbstractRelation relation) 
            throws UnexpectedTypeException {
        // must be some select query.
        if (!(relation instanceof SelectQueryOp)) {
            throw new UnexpectedTypeException("Not implemented yet.");
        }
        
        SelectQueryOp rewritten = new SelectQueryOp();
        SelectQueryOp sel = (SelectQueryOp) relation;
        List<SelectItem> selectList = sel.getSelectList();
        List<SelectItem> newInnerSelectList = new ArrayList<>();
        List<SelectItem> newOuterSelectList = new ArrayList<>();
        String innerTableAliasName = generateNextAliasName();
        for (SelectItem item : selectList) {
            if (!(item instanceof AliasedColumn)) {
                throw new UnexpectedTypeException("The following select item is not aliased: " + item.toString());
            }
            
            UnnamedColumn c = ((AliasedColumn) item).getColumn();
            String aliasName = ((AliasedColumn) item).getAliasName();
            
            if (c instanceof BaseColumn) {
                String aliasForBase = generateNextAliasName();
                newInnerSelectList.add(new AliasedColumn(c, aliasForBase));
                newOuterSelectList.add(new AliasedColumn(new BaseColumn(innerTableAliasName, aliasForBase), aliasName));
            }
            else if (c instanceof ColumnOp) {
                ColumnOp col = (ColumnOp) c;
                if (col.getOpType().equals("sum")) {
                    String aliasForSubSumEst = generateNextAliasName();
                    String aliasForSubsampleSize = generateNextAliasName();
                    String aliasForMeanEstimate = generateMeanEstimateAliasName(aliasName);
                    String aliasForStdEstimate = generateStdEstimateAliasName(aliasName);
                    UnnamedColumn op = col.getOperand();
                    UnnamedColumn probCol = deriveInclusionProbabilityColumn(relation);
                    ColumnOp newCol = new ColumnOp("sum",
                                          new ColumnOp("divide", Arrays.asList(op, probCol)));
                    newInnerSelectList.add(new AliasedColumn(newCol, aliasForSubSumEst));
                    newInnerSelectList.add(new AliasedColumn(new ColumnOp("count"), aliasForSubsampleSize));
                    newOuterSelectList.add(new AliasedColumn(
                                               new ColumnOp("sum", new BaseColumn(innerTableAliasName, aliasForSubSumEst)),
                                               aliasForMeanEstimate));
                    newOuterSelectList.add(new AliasedColumn(ColumnOp.divide(
                                                   new ColumnOp("std",
                                                       ColumnOp.multiply(
                                                           new BaseColumn(innerTableAliasName, aliasForSubSumEst),
                                                           ColumnOp.sqrt(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)))),
                                                   ColumnOp.sqrt(
                                                       ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)))),
                                               aliasForStdEstimate));
                    newInnerGroupbyList.add()
                }
                else if (col.getOpType().equals("count")) {
                    UnnamedColumn probCol = deriveInclusionProbabilityColumn(relation);
                    ColumnOp newCol = new ColumnOp("sum",
                                          new ColumnOp("divide", Arrays.asList(ConstantColumn.valueOf(1), probCol)));
                    newInnerSelectList.add(new AliasedColumn(newCol, aliasName));
                }
                else if (col.getOpType().equals("avg")) {
                    UnnamedColumn op = col.getOperand();
                    UnnamedColumn probCol = deriveInclusionProbabilityColumn(relation);
                    // sum of attribute values
                    ColumnOp newCol1 = new ColumnOp("sum",
                                         new ColumnOp("divide", Arrays.asList(op, probCol)));
                    // number of attribute values
                    ColumnOp oneIfNotNull = ColumnOp.casewhenelse(
                                                ConstantColumn.valueOf(1),
                                                ColumnOp.notnull(op),
                                                ConstantColumn.valueOf(0));
                    ColumnOp newCol2 = new ColumnOp("sum",
                                         new ColumnOp("divide", Arrays.asList(oneIfNotNull, probCol)));
                    newInnerSelectList.add(new AliasedColumn(newCol1, aliasName + "_sum"));
                    newInnerSelectList.add(new AliasedColumn(newCol2, aliasName + "_count"));
                }
                else {
                    throw new UnexpectedTypeException("Not implemented yet.");
                }
            }
            else {
                throw new UnexpectedTypeException("Unexpected column type: " + c.getClass().toString());
            }
        }
        
        for (SelectItem c : newInnerSelectList) {
            rewritten.addSelectItem(c);
        }
        for (AbstractRelation r : sel.getFromList()) {
            rewritten.addTableSource(r);
        }
        
        if (sel.getFilter().isPresent()) {
            rewritten.addFilterByAnd(sel.getFilter().get());
        }
        
        return rewritten;
    }
    
    List<UnnamedColumn> generatePartitionPredicates(AbstractRelation relation) throws VerdictDbException {
        if (!(relation instanceof SelectQueryOp)) {
            throw new UnexpectedTypeException("Unexpected relation type: " + relation.getClass().toString());
        }
        
        List<UnnamedColumn> partitionPredicates = new ArrayList<>();
        SelectQueryOp sel = (SelectQueryOp) relation;
        List<AbstractRelation> fromList = sel.getFromList();
        for (AbstractRelation r : fromList) {
            Optional<UnnamedColumn> c = partitionColumnOfSource(r);
            if (!c.isPresent()) {
                continue;
            }
            if (partitionPredicates.size() > 0) {
                throw new ValueException("Only a single table can be a scrambled table.");
            }
            
            UnnamedColumn partCol = c.get();
            List<String> partitionAttributeValues = partitionAttributeValuesOfSource(r);
            for (String v : partitionAttributeValues) {
                partitionPredicates.add(ColumnOp.equal(partCol, ConstantColumn.valueOf(v)));
            }
        }
        
        return partitionPredicates;
    }
    
    List<String> partitionAttributeValuesOfSource(AbstractRelation source) throws UnexpectedTypeException {
        if (source instanceof BaseTable) {
            BaseTable base = (BaseTable) source;
            String schemaName = base.getSchemaName();
            String tableName = base.getTableName();
            return scrambleMeta.getPartitionAttributes(schemaName, tableName);
        }
        else {
            throw new UnexpectedTypeException("Not implemented yet.");
        }
        
    }
    
//    ColumnOp derivePartitionFilter(AbstractRelation relation, int partitionNumber) throws UnexpectedTypeException {
//        AbstractColumn partCol = derivePartitionColumn(relation);
//        String partitionValue = derivePartitionValue(relation, partitionNumber);
//        return ColumnOp.equal(partCol, ConstantColumn.valueOf(partitionValue));
//    }
    
    Optional<UnnamedColumn> partitionColumnOfSource(AbstractRelation source) throws UnexpectedTypeException {
        if (source instanceof BaseTable) {
            BaseTable base = (BaseTable) source;
            String colName = scrambleMeta.getPartitionColumn(base.getSchemaName(), base.getTableName());
            String aliasName = base.getTableSourceAlias();
            BaseColumn col = new BaseColumn(aliasName, colName);
            return Optional.<UnnamedColumn>of(col);
        }
        else {
            throw new UnexpectedTypeException("Not implemented yet.");
        }
    }
    
//    int derivePartitionCount(AbstractRelation relation) throws UnexpectedTypeException {
//        if (!(relation instanceof SelectQueryOp)) {
//            throw new UnexpectedTypeException("Unexpected relation type: " + relation.getClass().toString());
//        }
//        // TODO: partition count should be modified to handle the joins of multiple tables.
//        SelectQueryOp sel = (SelectQueryOp) relation;
//        List<AbstractRelation> fromList = sel.getFromList();
//        int partCount = 0;
//        for (AbstractRelation r : fromList) {
//            int c = partitionCountOfSource(r);
//            if (partCount == 0) {
//                partCount = c;
//            }
//            else {
//                partCount = partCount * c;
//            }
//        }
//        return partCount;
//    }
    
//    int partitionCountOfSource(AbstractRelation source) throws UnexpectedTypeException {
//        if (source instanceof BaseTable) {
//            BaseTable tab = (BaseTable) source;
//            return scrambleMeta.getPartitionCount(tab.getSchemaName(), tab.getTableName());
//        }
//        else {
//            throw new UnexpectedTypeException("Not implemented yet.");
//        }
//    }
    
    /**
     * Obtains the inclusion probability expression needed for computing the aggregates within the given
     * relation.
     * 
     * @param relation
     * @return
     * @throws UnexpectedTypeException
     */
    UnnamedColumn deriveInclusionProbabilityColumn(AbstractRelation relation) throws UnexpectedTypeException {
        if (!(relation instanceof SelectQueryOp)) {
            throw new UnexpectedTypeException("Unexpected relation type: " + relation.getClass().toString());
        }
        
        SelectQueryOp sel = (SelectQueryOp) relation;
        List<AbstractRelation> fromList = sel.getFromList();
        UnnamedColumn incProbCol = null;
        for (AbstractRelation r : fromList) {
            Optional<UnnamedColumn> c = inclusionProbabilityColumnOfSource(r);
            if (!c.isPresent()) {
                continue;
            }
            if (incProbCol == null) {
                incProbCol = c.get();
            }
            else {
                incProbCol = new ColumnOp("multiply", Arrays.asList(incProbCol, c.get()));
            }
        }
        return incProbCol;
    }
    
    Optional<UnnamedColumn> inclusionProbabilityColumnOfSource(AbstractRelation source) throws UnexpectedTypeException {
        if (source instanceof BaseTable) {
            BaseTable base = (BaseTable) source;
            String colName = scrambleMeta.getInclusionProbabilityColumn(base.getSchemaName(), base.getTableName());
            if (colName == null) {
                return Optional.empty();
            }
            String aliasName = base.getTableSourceAlias();
            BaseColumn col = new BaseColumn(aliasName, colName);
            return Optional.<UnnamedColumn>of(col);
        }
        else {
            throw new UnexpectedTypeException("Derived tables cannot be used."); 
        }
    }

}