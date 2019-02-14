package org.verdictdb.core.scrambling;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.verdictdb.core.scrambling.ScramblingNode.computeConditionalProbabilityDistribution;
import static org.verdictdb.core.scrambling.StratifiedScramblingMethod.StratifiedGroupNode.GROUP_COUNT_COLUMN_ALIAS;

/**
 * Policy: 1. Tier 0: tuples containing rare groups. 2. Tier 1: other tuples
 *
 * <p>Rare groups: Bottom 15.9% percentile values for the selected column. Assume the frequencies
 * of values are in normal distribution, select values of which frequencies are less than one standard
 * deviation from average frequency.
 *
 * <p>Blocks for Tier 0: Tier 0 takes up to 50% of each block.
 *
 * <p>Blocks for Tier 1: Tier 1 takes the rest of the space in each block.
 *
 * @author Shucheng Zhong
 */
public class StratifiedScramblingMethod extends ScramblingMethodBase {

  protected String type = "stratified";

  private static final long serialVersionUID = 8120705615187201159L;

  // If at least k rows of each group should be selected, the leastSamplingSize = k*a,
  // where a is the uniform sampling ratio.
  private long leastSamplingSize = 15;

  // Let k be the least number of rows user expected to find in one block for a group;
  // Let a be the sampling ratio user specified.
  // Let B be the total block number of the scrambling table
  // tier 0: rare group - #row < k / a
  // tier 1~10: intermediate group. Intervals are divided equally from k/a to k/a*B
  // tier 11 : large group - #row > k/a*B
  // If B < 10, maximumTierNumber = B+2, the tiers for intermediate groups will be reduced
  private long maximumTierNumber = 12;

  private long tableSize = -1;

  double p0 = 0.5; // max portion for Tier 0; should be configured dynamically in the future

  private String scratchpadSchemaName;

  private List<List<Double>> tierCumulProbDistList = null;

  private List<String> stratifiedColumns = new ArrayList<>();

  private double sampleingRatio = 0.1;

  public static final String MAIN_TABLE_SOURCE_ALIAS_NAME = "t1";

  // public static final String ORIGINAL_TABLE_SOURCE_ALIAS_NAME = "t0";

  public static final String GROUP_INFO_TABLE_SOURCE_ALIAS_NAME = "t2";

  // public static final String ROW_NUMBER_COLUMN_ALIAS_NAME = "verdictdb_row_number";

  private int totalNumberOfblocks = -1;

  private int actualNumberOfBlocks = -1;

  private static VerdictDBLogger log =
      VerdictDBLogger.getLogger(StratifiedScramblingMethod.class);

  private static final int DEFAULT_MAX_BLOCK_COUNT = 100;

  public StratifiedScramblingMethod() {
    super(0, 0, 0);
  }

  public StratifiedScramblingMethod(long blockSize, String scratchpadSchemaName) {
    super(blockSize, DEFAULT_MAX_BLOCK_COUNT, 1.0);
    this.scratchpadSchemaName = scratchpadSchemaName;
    this.type = "stratified";
  }

  public StratifiedScramblingMethod(
      long blockSize, String scratchpadSchemaName, long leastSamplingSize, double sampleingRatio) {
    this(blockSize, scratchpadSchemaName);
    this.sampleingRatio = sampleingRatio;
    this.leastSamplingSize = (long) Math.ceil(leastSamplingSize / sampleingRatio);
    this.type = "stratified";
  }

  public void setStratifiedColumns(List<String> stratifiedColumns) {
    this.stratifiedColumns = stratifiedColumns;
  }

  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName,
      String oldTableName,
      String columnMetaTokenKey,
      String partitionMetaTokenKey,
      String primarykeyMetaTokenKey) {

    List<ExecutableNodeBase> statisticsNodes = new ArrayList<>();

    // Group Count
    TempIdCreatorInScratchpadSchema idCreator =
        new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    StratifiedGroupNode sg = new StratifiedGroupNode(
        idCreator, oldSchemaName, oldTableName, stratifiedColumns, blockSize);
    statisticsNodes.add(sg);

    // Table Size Count Node
    TableSizeCountNode tc = new TableSizeCountNode(oldSchemaName, oldTableName);
    statisticsNodes.add(tc);

    return statisticsNodes;
  }

  private UnnamedColumn createRareGroupTuplePredicate(
      String sourceTableAlias) {
    boolean printLog = false;
    return createRareGroupTuplePredicate(sourceTableAlias, printLog);
  }

  private UnnamedColumn createRareGroupTuplePredicate(
      String sourceTableAlias, boolean printLog) {
    if (printLog) {
      log.info(
          String.format(
              "In stratified columns, the groups of which sizes are below %d"
                  + "will be prioritized in future query processing.",
              leastSamplingSize));
    }

    UnnamedColumn outlierPredicate = ColumnOp.less(
        new BaseColumn(sourceTableAlias, GROUP_COUNT_COLUMN_ALIAS),
        ConstantColumn.valueOf(leastSamplingSize));
    return outlierPredicate;
  }

  private UnnamedColumn createOtherGroupTuplePredicate(String sourceTableAlias, long upperBound) {
    UnnamedColumn outlierPredicate = ColumnOp.less(
        new BaseColumn(sourceTableAlias, GROUP_COUNT_COLUMN_ALIAS),
        ConstantColumn.valueOf(upperBound));
    return outlierPredicate;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData) {
    retrieveTableSizeAndBlockNumber(metaData);
    maximumTierNumber = Math.min(actualNumberOfBlocks + 2, maximumTierNumber);

    List<UnnamedColumn> predictList = new ArrayList<>();
    // Tier 0
    UnnamedColumn tier0Predicate =
        createRareGroupTuplePredicate(StratifiedScramblingMethod.GROUP_INFO_TABLE_SOURCE_ALIAS_NAME);
    predictList.add(tier0Predicate);

    long maxThreshold = leastSamplingSize * totalNumberOfblocks;
    for (int i = 0; i < (maximumTierNumber - 2); i++) {
      UnnamedColumn tierPredictate = createOtherGroupTuplePredicate(
          StratifiedScramblingMethod.GROUP_INFO_TABLE_SOURCE_ALIAS_NAME,
          (long) Math.ceil((double) maxThreshold / (double) (maximumTierNumber - 2)) * (i + 1));
      predictList.add(tierPredictate);
    }
    // Tier 1: automatically handled by this function's caller
    return predictList;
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, int tier) {


    // this cumulative prob is calculated at the first call of this method
    if (tierCumulProbDistList == null) {
      tierCumulProbDistList = new ArrayList<>();
      populateAllCumulativeProbabilityDistribution(metaData);
    }

    List<Double> dist;

    dist = tierCumulProbDistList.get(tier);

    if (actualNumberOfBlocks != totalNumberOfblocks) {
      storeCumulativeProbabilityDistribution(tier, dist.subList(0, actualNumberOfBlocks));
    } else {
      storeCumulativeProbabilityDistribution(tier, dist);
    }
    return dist;
  }

  private void populateAllCumulativeProbabilityDistribution(Map<String, Object> metaData) {
    populateTier0CumulProbDist(metaData);
    for (int i = 0; i < maximumTierNumber - 2; i++) {
      populateIntermediateTierCumulProbDist(metaData, i);
    }
    populateLastTierCumulProbDist(metaData);
  }

  /*
  private long calcuteEvenBlockSize(int totalNumberOfblocks, long tableSize) {
    return (long) Math.round((float) tableSize / (float) totalNumberOfblocks);
  }
  */

  /**
   * Probability distribution for Tier0: All rare groups are put into block0.
   *
   * @param metaData
   */
  private void populateTier0CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    for (int i = 0; i < totalNumberOfblocks; i++) {
      cumulProbDist.add(1.0);
    }
    tierCumulProbDistList.add(cumulProbDist);
  }

  /**
   * Probability distribution for last tier:
   * groups that are large enough to do uniform sampling for all blocks
   *
   * @param metaData
   */
  private void populateLastTierCumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();
    double uniformSamplingRatio = 1.0 / totalNumberOfblocks;
    cumulProbDist.add(uniformSamplingRatio);
    for (int i = 1; i < totalNumberOfblocks - 1; i++) {
      cumulProbDist.add(cumulProbDist.get(i - 1) + uniformSamplingRatio);
    }
    for (int i = totalNumberOfblocks - 1; i < totalNumberOfblocks; i++) {
      cumulProbDist.add(1.0);
    }
    tierCumulProbDistList.add(cumulProbDist);
  }

  /**
   * Probability distribution for Intermediate tiers: the tuples that do not belong to tier0 and last tier.
   * Uniform sampling within in first (idx/(maximumTierNumber-2))*totalNumberOfblocks blocks.
   *
   * @param metaData
   */
  private void populateIntermediateTierCumulProbDist(Map<String, Object> metaData, int idx) {
    List<Double> cumulProbDist = new ArrayList<>();
    int uniformBlockdCount = (int) Math.ceil(((double)idx/(double)(maximumTierNumber - 2))*totalNumberOfblocks);
    // Prevent 1 divide 0 case.
    if (uniformBlockdCount == 0) {
      uniformBlockdCount = 1;
    }
    double uniformSamplingRatio = 1.0 / uniformBlockdCount;
    if (uniformBlockdCount!=1) {
      cumulProbDist.add(uniformSamplingRatio);
    }
    for (int i = 1; i < uniformBlockdCount - 1; i++) {
      cumulProbDist.add(cumulProbDist.get(i - 1) + uniformSamplingRatio);
    }
    for (int i = uniformBlockdCount - 1; i < totalNumberOfblocks; i++) {
      cumulProbDist.add(1.0);
    }
    tierCumulProbDistList.add(cumulProbDist);
  }


  // Helper
  // Block number of stratified sampling is (1 / sampling ratio). All rare groups go to block 0.
  private void retrieveTableSizeAndBlockNumber(Map<String, Object> metaData) {
    DbmsQueryResult tableSizeResult =
        (DbmsQueryResult) metaData.get(TableSizeCountNode.class.getSimpleName());
    tableSizeResult.rewind();
    tableSizeResult.next();
    tableSize = tableSizeResult.getLong(TableSizeCountNode.TOTAL_COUNT_ALIAS_NAME);
    totalNumberOfblocks = (int) Math.ceil(tableSize / (float) blockSize);
    actualNumberOfBlocks = (int) Math.ceil(totalNumberOfblocks * sampleingRatio);
  }

  @Override
  public AbstractRelation getScramblingSource(
      String originalSchema, String originalTable, Map<String, Object> metaData) {
    @SuppressWarnings("unchecked")
    String groupDistributionSchemaName = (String) metaData.get("schemaName");
    String groupDistributionTableName = (String) metaData.get("tableName");

    UnnamedColumn joinPredicate = null;
    for (String columnName : stratifiedColumns) {
      if (joinPredicate == null) {
        joinPredicate = ColumnOp.equal(
            new BaseColumn(MAIN_TABLE_SOURCE_ALIAS_NAME, columnName),
            new BaseColumn(GROUP_INFO_TABLE_SOURCE_ALIAS_NAME, columnName));
      } else {
        joinPredicate = ColumnOp.and(
            joinPredicate,
            ColumnOp.equal(
                new BaseColumn(MAIN_TABLE_SOURCE_ALIAS_NAME, columnName),
                new BaseColumn(GROUP_INFO_TABLE_SOURCE_ALIAS_NAME, columnName)));
      }
    }
    JoinTable source =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(
                new BaseTable(originalSchema, originalTable, MAIN_TABLE_SOURCE_ALIAS_NAME),
                new BaseTable(
                    groupDistributionSchemaName,
                    groupDistributionTableName,
                    GROUP_INFO_TABLE_SOURCE_ALIAS_NAME)),
            Arrays.asList(JoinTable.JoinType.leftouter),
            Arrays.asList(joinPredicate));
    /*
    UnnamedColumn joinPredicate = null;
    for (String columnName : stratifiedColumns) {
      if (joinPredicate == null) {
        joinPredicate = ColumnOp.equal(
            new BaseColumn(ORIGINAL_TABLE_SOURCE_ALIAS_NAME, columnName),
            new BaseColumn(GROUP_INFO_TABLE_SOURCE_ALIAS_NAME, columnName));
      } else {
        joinPredicate = ColumnOp.and(
            joinPredicate,
            ColumnOp.equal(
                new BaseColumn(ORIGINAL_TABLE_SOURCE_ALIAS_NAME, columnName),
                new BaseColumn(GROUP_INFO_TABLE_SOURCE_ALIAS_NAME, columnName)));
      }
    }
    JoinTable source =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(
                new BaseTable(originalSchema, originalTable, ORIGINAL_TABLE_SOURCE_ALIAS_NAME),
                new BaseTable(
                    groupDistributionSchemaName,
                    groupDistributionTableName,
                    GROUP_INFO_TABLE_SOURCE_ALIAS_NAME)),
            Arrays.asList(JoinTable.JoinType.leftouter),
            Arrays.asList(joinPredicate));

    // create temp table for row number
    List<SelectItem> selectItemList = new ArrayList<>();
    for (Pair<String, String> columns : (List<Pair>) metaData.get("scramblingPlan:columnMetaData")) {
      selectItemList.add(new BaseColumn(ORIGINAL_TABLE_SOURCE_ALIAS_NAME, columns.getKey()));
    }
    List<UnnamedColumn> stratifiedColumnsList = new ArrayList<>();
    for (String columnName : stratifiedColumns) {
      stratifiedColumnsList.add(new BaseColumn(ORIGINAL_TABLE_SOURCE_ALIAS_NAME, columnName));
    }
    selectItemList.add(new AliasedColumn(new ColumnOp("rownumber", stratifiedColumnsList), ROW_NUMBER_COLUMN_ALIAS_NAME));
    selectItemList.add(new BaseColumn(GROUP_INFO_TABLE_SOURCE_ALIAS_NAME, GROUP_COUNT_COLUMN_ALIAS));
    SelectQuery selectQuery = SelectQuery.create(selectItemList, source);
    selectQuery.setAliasName(MAIN_TABLE_SOURCE_ALIAS_NAME);
    */
    return source;
  }

  @Override
  public String getMainTableAlias() {
    return MAIN_TABLE_SOURCE_ALIAS_NAME;
  }

  @Override
  public UnnamedColumn getBlockExprForTier(int tier, Map<String, Object> metaData) {

    List<Double> cumulProb = getCumulativeProbabilityDistributionForTier(metaData, tier);
    List<Double> condProb = computeConditionalProbabilityDistribution(cumulProb);
    int blockCount = cumulProb.size();

    List<UnnamedColumn> blockForTierOperands = new ArrayList<>();
    for (int j = 0; j < blockCount; j++) {
      // ensure k rows for non-rare group(tier 1)
      blockForTierOperands.add(
          ColumnOp.lessequal(ColumnOp.rand(), ConstantColumn.valueOf(condProb.get(j))));
      blockForTierOperands.add(ConstantColumn.valueOf(j));
    }
    UnnamedColumn blockForTierExpr;

    if (blockForTierOperands.size() <= 1) {
      blockForTierExpr = ConstantColumn.valueOf(0);
    } else {
      blockForTierExpr = ColumnOp.casewhen(blockForTierOperands);
    }

    return blockForTierExpr;
  }

  @Override
  public int getBlockCount() {
    return totalNumberOfblocks;
  }

  @Override
  public int getActualBlockCount() {
    return actualNumberOfBlocks;
  }

  @Override
  public int getTierCount() {
    return 2;
  }

  @Override
  public double getRelativeSize() {
    return relativeSize;
  }


  /**
   * Create temporary table store the group size of the original table.
   * <p>
   * CREATE [stratified_group_table]
   * AS SELECT
   * [stratified columns],
   * count(*)
   * FROM
   * [original table]
   * GROUP BY [startified columns]
   */
  class StratifiedGroupNode extends CreateTableAsSelectNode {

    private String schemaName;

    private String tableName;

    private List<String> stratifiedColumnNames;

    private long blockSize;

    private static final long serialVersionUID = -2881242011123433574L;

    public static final String GROUP_COUNT_COLUMN_ALIAS = "verdictdb_group_cnt";

    public StratifiedGroupNode(
        IdCreator idCreator,
        String schemaName,
        String tableName,
        List<String> stratifiedColumnNames,
        long blockSize) {
      super(idCreator, null);
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.stratifiedColumnNames = stratifiedColumnNames;
      this.blockSize = blockSize;
    }

    @Override
    public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
      String tableSourceAlias = "t";

      // select
      List<SelectItem> selectList = new ArrayList<>();
      for (String columnName : stratifiedColumnNames) {
        selectList.add(
            new AliasedColumn(
                new BaseColumn(tableSourceAlias, columnName), columnName));
      }
      selectList.add(new AliasedColumn(ColumnOp.count(), GROUP_COUNT_COLUMN_ALIAS));

      // from
      SelectQuery selectQuery =
          SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));

      // group by
      for (String columnName : stratifiedColumnNames) {
        selectQuery.addGroupby(new AliasReference(columnName));
      }

      this.selectQuery = selectQuery;
      return super.createQuery(tokens);
    }

    @Override
    public ExecutionInfoToken createToken(DbmsQueryResult result) {
      ExecutionInfoToken token = super.createToken(result);
      Pair<String, String> fullTableName =
          Pair.of((String) token.getValue("schemaName"), (String) token.getValue("tableName"));

      // set duplicate information for convenience
      token.setKeyValue(this.getClass().getSimpleName(), fullTableName);
      return token;
    }

  }
}
