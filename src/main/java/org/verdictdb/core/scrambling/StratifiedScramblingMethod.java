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
import static org.verdictdb.core.scrambling.StratifiedScramblingMethod.GroupDistributionNode.GROUP_COUNT_COLUMN_ALIAS;

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

  double p0 = 0.5; // max portion for Tier 0; should be configured dynamically in the future

  static final double RARE_GROUP_STDDEV_MULTIPLIER = 1.0;

  private String primaryColumnName = null;

  private String scratchpadSchemaName;

  private List<Double> tier0CumulProbDist = null;

  private List<Double> tier1CumulProbDist = null;

  private List<String> stratifiedColumns = new ArrayList<>();

  private long rareGroupSize = 0; // tier 0 size

  public static final String MAIN_TABLE_SOURCE_ALIAS_NAME = "t1";

  public static final String GROUP_INFO_TABLE_SOURCE_ALIAS_NAME = "t2";

  private int totalNumberOfblocks = -1;

  private static VerdictDBLogger log =
      VerdictDBLogger.getLogger(FastConvergeScramblingMethod.class);

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
      long blockSize, String scratchpadSchemaName, String primaryColumnName) {
    this(blockSize, scratchpadSchemaName);
    this.primaryColumnName = primaryColumnName;
    this.type = "stratified";
  }

  public StratifiedScramblingMethod(
      Map<Integer, List<Double>> probDist, String primaryColumnName) {
    super(probDist);
    this.type = "stratified";
    this.primaryColumnName = primaryColumnName;
    this.tier0CumulProbDist = probDist.get(0);
    this.tier1CumulProbDist = probDist.get(1);
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

    // Group Count info
    TempIdCreatorInScratchpadSchema idCreator =
        new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    StratifiedGroupNode sg = new StratifiedGroupNode(
        idCreator, oldSchemaName, oldTableName, stratifiedColumns, blockSize);
    statisticsNodes.add(sg);

    // Group Distribution
    GroupDistributionNode gd = new GroupDistributionNode();
    sg.registerSubscriber(gd.getSubscriptionTicket());
    statisticsNodes.add(gd);

    // Rare Group Size
    RareGroupSizeNode rg = new RareGroupSizeNode();
    rg.subscribeTo(gd);
    statisticsNodes.add(rg);

    // Table Size Count Node
    TableSizeCountNode tc = new TableSizeCountNode(oldSchemaName, oldTableName);
    statisticsNodes.add(tc);

    return statisticsNodes;
  }

  static UnnamedColumn createRareGroupTuplePredicate(
      Map<String, Object> metaData, String sourceTableAlias) {
    boolean printLog = false;
    return createRareGroupTuplePredicate(metaData, sourceTableAlias, printLog);
  }

  static UnnamedColumn createRareGroupTuplePredicate(
      Map<String, Object> metaData, String sourceTableAlias, boolean printLog) {
    DbmsQueryResult GroupDistributionResult =
        (DbmsQueryResult) metaData.get(GroupDistributionNode.class.getSimpleName());

    GroupDistributionResult.rewind();
    GroupDistributionResult.next(); // assumes that the original table has at least one row.
    double groupSizeAvg = GroupDistributionResult.getDouble(0);
    double groupSizeStddev = GroupDistributionResult.getDouble(1);
    double lowCriteria = groupSizeAvg - groupSizeStddev * RARE_GROUP_STDDEV_MULTIPLIER;
    if (printLog) {
      log.info(
          String.format(
              "In stratified columns, the groups of which sizes are below %.2f"
                  + "will be prioritized in future query processing.",
              lowCriteria));
    }

    UnnamedColumn outlierPredicate = ColumnOp.less(
        new BaseColumn(sourceTableAlias, GROUP_COUNT_COLUMN_ALIAS),
        ConstantColumn.valueOf(lowCriteria));
    return outlierPredicate;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData) {
    // Tier 0
    UnnamedColumn tier0Predicate =
        createRareGroupTuplePredicate(
            metaData, StratifiedScramblingMethod.GROUP_INFO_TABLE_SOURCE_ALIAS_NAME);

    // Tier 1: automatically handled by this function's caller
    return Arrays.asList(tier0Predicate);
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, int tier) {
    // this cumulative prob is calculated at the first call of this method
    if (tier0CumulProbDist == null) {
      populateAllCumulativeProbabilityDistribution(metaData);
    }

    List<Double> dist;

    if (tier == 0) {
      dist = tier0CumulProbDist;
    } else {
      dist = tier1CumulProbDist;
    }
    storeCumulativeProbabilityDistribution(tier, dist);
    return dist;
  }

  private void populateAllCumulativeProbabilityDistribution(Map<String, Object> metaData) {
    populateTier0CumulProbDist(metaData);
    populateTier1CumulProbDist(metaData);
  }

  private long calcuteEvenBlockSize(int totalNumberOfblocks, long tableSize) {
    return (long) Math.round((float) tableSize / (float) totalNumberOfblocks);
  }

  private void populateTier0CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Integer> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    int totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    long evenBlockSize = calcuteEvenBlockSize(totalNumberOfblocks, tableSize);

    DbmsQueryResult rareGroupProportion =
        (DbmsQueryResult) metaData.get(RareGroupSizeNode.class.getSimpleName());
    rareGroupProportion.rewind();
    rareGroupProportion.next();
    rareGroupSize = rareGroupProportion.getLong(0);

    if (rareGroupSize * 2 >= tableSize) {
      // too large outlier -> no special treatment
      for (int i = 0; i < totalNumberOfblocks; i++) {
        cumulProbDist.add((i + 1) / (double) totalNumberOfblocks);
      }

    } else {
      Long remainingSize = rareGroupSize;

      while (rareGroupSize > 0 && remainingSize > 0) {
        // fill only p0 portion of each block at most
        if (remainingSize <= p0 * evenBlockSize) {
          cumulProbDist.add(1.0);
          break;
        } else {
          long thisBlockSize = (long) (evenBlockSize * p0);
          double ratio = thisBlockSize / (float) rareGroupSize;
          if (cumulProbDist.size() == 0) {
            cumulProbDist.add(ratio);
          } else {
            cumulProbDist.add(cumulProbDist.get(cumulProbDist.size() - 1) + ratio);
          }
          remainingSize -= thisBlockSize;
        }
      }

      // in case where the length of the prob distribution is not equal to the total block number
      while (cumulProbDist.size() < totalNumberOfblocks) {
        cumulProbDist.add(1.0);
      }
    }

    tier0CumulProbDist = cumulProbDist;
  }

  /**
   * Probability distribution for Tier1: the tuples that do not belong to tier0.
   *
   * @param metaData
   */
  private void populateTier1CumulProbDist(Map<String, Object> metaData) {
    List<Double> cumulProbDist = new ArrayList<>();

    // calculate the number of blocks
    Pair<Long, Integer> tableSizeAndBlockNumber = retrieveTableSizeAndBlockNumber(metaData);
    long tableSize = tableSizeAndBlockNumber.getLeft();
    int totalNumberOfblocks = tableSizeAndBlockNumber.getRight();
    long evenBlockSize = calcuteEvenBlockSize(totalNumberOfblocks, tableSize);

    //    System.out.println("table size: " + tableSize);
    //    System.out.println("outlier size: " + rareGroupSize);
    //    System.out.println("small group size: " + smallGroupSizeSum);
    long tier2Size = tableSize - rareGroupSize;

    for (int i = 0; i < totalNumberOfblocks; i++) {
      long thisTier0Size;
      if (i == 0) {
        thisTier0Size = (long) (rareGroupSize * tier0CumulProbDist.get(i));
      } else {
        thisTier0Size =
            (long) (rareGroupSize * (tier0CumulProbDist.get(i) - tier0CumulProbDist.get(i - 1)));
      }
      long thisBlockSize = evenBlockSize - thisTier0Size;
      if (tier2Size == 0) {
        cumulProbDist.add(1.0);
      } else {
        double thisBlockRatio = thisBlockSize / (double) tier2Size;
        if (i == 0) {
          cumulProbDist.add(thisBlockRatio);
        } else if (i == totalNumberOfblocks - 1) {
          cumulProbDist.add(1.0);
        } else {
          cumulProbDist.add(Math.min(cumulProbDist.get(i - 1) + thisBlockRatio, 1.0));
        }
      }
    }

    tier1CumulProbDist = cumulProbDist;
  }

  // Helper
  private Pair<Long, Integer> retrieveTableSizeAndBlockNumber(Map<String, Object> metaData) {
    DbmsQueryResult tableSizeResult =
        (DbmsQueryResult) metaData.get(TableSizeCountNode.class.getSimpleName());
    tableSizeResult.rewind();
    tableSizeResult.next();
    long tableSize = tableSizeResult.getLong(TableSizeCountNode.TOTAL_COUNT_ALIAS_NAME);
    totalNumberOfblocks = (int) Math.ceil(tableSize / (float) blockSize);
    return Pair.of(tableSize, totalNumberOfblocks);
  }

  @Override
  public AbstractRelation getScramblingSource(
      String originalSchema, String originalTable, Map<String, Object> metaData) {
    @SuppressWarnings("unchecked")
    String groupDistributionSchemaName = (String) metaData.get("schemaName");
    String groupDistributionTableName = (String) metaData.get("tableName");

    UnnamedColumn joinPredicate = null;
    for (String columnName:stratifiedColumns) {
      if (joinPredicate==null) {
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
    return totalNumberOfblocks;
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
   *
   * CREATE [stratified_group_table]
   * AS SELECT
   *   [stratified columns],
   *   count(*)
   * FROM
   *   [original table]
   * GROUP BY [startified columns]
   *
   */
  class StratifiedGroupNode extends CreateTableAsSelectNode {

    private String schemaName;

    private String tableName;

    private List<String> stratifiedColumnNames;

    private long blockSize;

    private static final long serialVersionUID = -2881242011123433574L;

    private static final String GROUP_COUNT_COLUMN_ALIAS = "verdictdb_group_cnt";

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


  /**
   * This node is used to find the average group size and the standard deviation
   * from the temporary table created in StratifiedGroupNode.
   *
   * SELECT
   *   avg(GROUP_COUNT_COLUMN_ALIAS),
   *   stddev(GROUP_COUNT_COLUMN_ALIAS)
   * FROM [stratified_group_table]
   *
   */
  class GroupDistributionNode extends QueryNodeWithPlaceHolders {

    private static final long serialVersionUID = 3881241211123433574L;

    private static final String GROUP_SIZE_AVG_ALIAS = "verdictdb_group_avg";

    private static final String GROUP_SIZE_STDDEV_ALIAS = "verdictdb_group_stddev";

    static final String GROUP_COUNT_COLUMN_ALIAS = "verdictdb_group_cnt";

    // When this node subscribes to the downstream nodes, this information must be used.
    private SubscriptionTicket subscriptionTicket;

    public GroupDistributionNode() {
      super(-1, null);

      // create a selectQuery for this node
      String tableSourceAlias = "t";

      Pair<BaseTable, SubscriptionTicket> placeholder = createPlaceHolderTable(tableSourceAlias);
      BaseTable baseTable = placeholder.getLeft();
      List<SelectItem> selectItemList = new ArrayList<>();
      selectItemList.add(new AliasedColumn(
          ColumnOp.avg(new BaseColumn(tableSourceAlias, GROUP_COUNT_COLUMN_ALIAS)), GROUP_SIZE_AVG_ALIAS));
      selectItemList.add(new AliasedColumn(
          ColumnOp.std(new BaseColumn(tableSourceAlias, GROUP_COUNT_COLUMN_ALIAS)), GROUP_SIZE_STDDEV_ALIAS));
      selectQuery = SelectQuery.create(selectItemList, baseTable);
      subscriptionTicket = placeholder.getRight();
    }

    public SubscriptionTicket getSubscriptionTicket() {
      return subscriptionTicket;
    }

    @Override
    public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
      this.selectQuery = (SelectQuery) super.createQuery(tokens);
      return this.selectQuery;
    }

    @Override
    public ExecutionInfoToken createToken(DbmsQueryResult result) {
      ExecutionInfoToken token = new ExecutionInfoToken();
      token.setKeyValue(this.getClass().getSimpleName(), result);
      BaseTable t = ((BaseTable) this.selectQuery.getFromList().get(0));
      token.setKeyValue("schemaName", t.getSchemaName());
      token.setKeyValue("tableName", t.getTableName());
      return token;
    }
  }

  /**
   * This node counts the size of rare groups. Rare groups are defined to be
   * groups of which size is below average by certain value. e.g:
   * [Group size] < [Average Group Size] - RareGroupMultiplier * [Group Size Standard Deviation]
   *
   * SELECT
   *   sum(GROUP_COUNT_COLUMN_ALIAS)
   * FROM
   *   [stratified_group_table]
   * WHERE
   *   [some predicate to distinguish rare group]
   *
   */
  class RareGroupSizeNode extends QueryNodeWithPlaceHolders {

    private double RareGroupMultiplier = 1.0;

    private static final long serialVersionUID = -2883642011673433574L;

    private static final String RARE_GROUP_SIZE_ALIAS = "verdictdb_rg_size";

    public RareGroupSizeNode() {
      super(-1, null);
    }

    /**
     * Select sum(GROUP_COUNT_COLUMN_ALIAS) from [stratified_group_table]
     * where [some predicate to distinguish rare group]
     */
    @Override
    public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
      ExecutionInfoToken token = tokens.get(0);

      String tableSourceAlias = "t";
      BaseTable baseTable = new BaseTable(
          (String)token.getValue("schemaName"),
          (String)token.getValue("tableName"),
          tableSourceAlias);
      selectQuery =
          SelectQuery.create(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(tableSourceAlias, GROUP_COUNT_COLUMN_ALIAS)), RARE_GROUP_SIZE_ALIAS),
              baseTable);
      DbmsQueryResult groupSizeInfo = (DbmsQueryResult) token.getValue(GroupDistributionNode.class.getSimpleName());
      groupSizeInfo.rewind();
      groupSizeInfo.next();
      Double averageGroupSize = groupSizeInfo.getDouble(0);
      Double stdGroupSize = groupSizeInfo.getDouble(1);
      selectQuery.addFilterByAnd(ColumnOp.less(
          new BaseColumn(tableSourceAlias, GROUP_COUNT_COLUMN_ALIAS),
          ConstantColumn.valueOf(averageGroupSize - RareGroupMultiplier * stdGroupSize)));
      return selectQuery;
    }

    @Override
    public ExecutionInfoToken createToken(DbmsQueryResult result) {
      ExecutionInfoToken token = new ExecutionInfoToken();
      token.setKeyValue(this.getClass().getSimpleName(), result);
      return token;
    }
  }

}
