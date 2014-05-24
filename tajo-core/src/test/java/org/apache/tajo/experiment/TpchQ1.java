package org.apache.tajo.experiment;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.physical.PhysicalExec;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.map.*;
import org.apache.tajo.storage.vector.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.Test;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.VecRowParquetReader;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TpchQ1 {

  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static AbstractStorageManager sm;
  private static Path testDir;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  private static MasterPlan masterPlan;

  public TpchQ1() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir("target/test-data/TestPhysicalPlanner");
    sm = StorageManagerFactory.getStorageManager(conf, testDir);
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);

    masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);
  }

  public void shutdown() {
    util.shutdownCatalogCluster();
  }

  static final Schema LINEITEM;

  static {
    LINEITEM = new Schema()
        .addColumn("l_orderkey", TajoDataTypes.Type.INT4) // 0
        .addColumn("l_partkey", TajoDataTypes.Type.INT4) // 1
        .addColumn("l_suppkey", TajoDataTypes.Type.INT4) // 2
        .addColumn("l_linenumber", TajoDataTypes.Type.INT4) // 3
        .addColumn("l_quantity", TajoDataTypes.Type.FLOAT8) // 4
        .addColumn("l_extendedprice", TajoDataTypes.Type.FLOAT8) // 5
        .addColumn("l_discount", TajoDataTypes.Type.FLOAT8) // 6
        .addColumn("l_tax", TajoDataTypes.Type.FLOAT8) // 7
        .addColumn("l_returnflag", TajoDataTypes.Type.CHAR, 1) // 8
        .addColumn("l_linestatus", TajoDataTypes.Type.CHAR, 1) // 9
        .addColumn("l_shipdate", TajoDataTypes.Type.CHAR, 10) // 10
        .addColumn("l_commitdate", TajoDataTypes.Type.CHAR, 10) // 11
        .addColumn("l_receiptdate", TajoDataTypes.Type.CHAR, 10) // 12
        .addColumn("l_shipinstruct", TajoDataTypes.Type.CHAR, 10) // 13
        .addColumn("l_shipmode", TajoDataTypes.Type.TEXT) // 14
        .addColumn("l_comment", TajoDataTypes.Type.TEXT); // 15
  }

  @Test
  public void generateTuples() throws IOException, SQLException, ServiceException, PlanningException {
    Path rawLineitemPath = new Path("file:///Users/hyunsik/Code/tpch_2_15_0/dbgen/lineitem.tbl");

    KeyValueSet kv = new KeyValueSet();
    kv.put(StorageConstants.CSVFILE_DELIMITER, "\\|");
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.CSV, kv);

    TableDesc tabledesc = new TableDesc("default.lineitem", LINEITEM, meta, rawLineitemPath, true);
    catalog.createTable(tabledesc);


    FileSystem localFS = FileSystem.getLocal(conf);

    long length = localFS.getLength(rawLineitemPath);

    FileFragment[] frags = new FileFragment[1];
    frags[0] = new FileFragment("default.lineitem", rawLineitemPath, 0, length, null);

    Path workDir = CommonTestingUtil.getTestDir("file:///Users/hyunsik/experiment/test-data");

    TaskAttemptContext ctx = new TaskAttemptContext(conf, LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setOutputPath(new Path("file:///Users/hyunsik/experiment/data/lineitem.parquet"));
    ctx.setEnforcer(new Enforcer());
    int blockSize =  1024 * 1024 * 256;
    Expr context = analyzer.parse("insert overwrite into location 'file:///home/hyunsik/lineitem.parquet' USING PARQUET WITH ('parquet.block.size' = '" + blockSize + "') SELECT * FROM default.lineitem");

    LogicalPlan plan = planner.createPlan(session, context);
    optimizer.optimize(plan);
    LogicalNode rootNode = plan.getRootBlock().getRoot();
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    while(exec.next() != null) {
    }
    exec.close();
  }

  @Test
  public void processQ1InTupleWay() throws IOException, SQLException, ServiceException, PlanningException {
    Path rawLineitemPath = new Path("file:///Users/hyunsik/experiment/data/lineitem.parquet");

    KeyValueSet kv = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = new TableMeta(CatalogProtos.StoreType.PARQUET, kv);

    TableDesc tabledesc = new TableDesc("default.lineitem", LINEITEM, meta, rawLineitemPath, true);
    catalog.createTable(tabledesc);

    FileSystem localFS = FileSystem.getLocal(conf);
    long length = localFS.getLength(rawLineitemPath);

    FileFragment[] frags = new FileFragment[1];
    frags[0] = new FileFragment("default.lineitem", rawLineitemPath, 0, length, null);
    Path workDir = CommonTestingUtil.getTestDir("file:///Users/hyunsik/experiment/test-data");

    Expr expr = analyzer.parse(FileUtil.readTextFileFromResource("experiment/q1.sql"));
    LogicalPlan plan = planner.createPlan(session, expr);
    optimizer.optimize(plan);

    Enforcer enforcer = new Enforcer();
    GroupbyNode groupbyNode = PlannerUtil.findTopNode(plan.getRootBlock().getRoot(), NodeType.GROUP_BY);
    for (AggregationFunctionCallEval eval : groupbyNode.getAggFunctions()) {
      eval.setFirstPhase();
    }
    enforcer.enforceHashAggregation(groupbyNode.getPID());

    TaskAttemptContext ctx = new TaskAttemptContext(conf, LocalTajoTestingUtility.newQueryUnitAttemptId(masterPlan),
        new FileFragment[] { frags[0] }, workDir);
    ctx.setOutputPath(new Path("file:///Users/hyunsik/experiment/test-data"));
    ctx.setEnforcer(enforcer);

    System.out.println(plan);

    LogicalNode rootNode = plan.getRootBlock().getRoot();
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();

    long beginProcess = System.currentTimeMillis();
    int resultCount = 0;
    while(exec.next() != null) {
      resultCount++;
    }
    long endProcess = System.currentTimeMillis();
    exec.close();
    System.out.println((endProcess - beginProcess) + " msec (" + resultCount + " rows)");
  }

  @Test
  public void testQ1InVectorization() throws IOException {
    KeyValueSet KeyValueSet = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.PARQUET, KeyValueSet);
    meta.putOption(ParquetOutputFormat.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name());

    Set<String> projectedNames = Sets.newHashSet(
        "l_returnflag",
        "l_linestatus",
        "l_quantity",
        "l_extendedprice",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_shipdate"
    );



    Schema projected = new Schema();
    for (Column p : LINEITEM.toArray()) {
      if (projectedNames.contains(p.getQualifiedName())) {
        projected.addColumn(p);
      }
    }

    System.out.println(">>>>>>" + projected);

    Path path = new Path("file:///Users/hyunsik/experiment/data/lineitem.parquet");
    VecRowBlock vecRowBlock = new VecRowBlock(projected, 1024);
    VecRowParquetReader reader = new VecRowParquetReader(path, LINEITEM, projected);
    long readStart = System.currentTimeMillis();

    //{(7) l_quantity (FLOAT8),l_extendedprice (FLOAT8),l_discount (FLOAT8),l_tax (FLOAT8),l_returnflag (CHAR(1)),l_linestatus (CHAR(1)),l_shipdate (CHAR(10))}

    int rowIdx = 0;
    int [] selVec = new int[vecRowBlock.maxVecSize()];
    int count = 0;
    long one_minus_l_discount = UnsafeUtil.allocVector(TajoDataTypes.Type.FLOAT8, 1024);
    long l_extendedprice_mul_one_minus_l_discount_ptr = UnsafeUtil.allocVector(TajoDataTypes.Type.FLOAT8, 1024);
    long one_plus_l_tax = UnsafeUtil.allocVector(TajoDataTypes.Type.FLOAT8, 1024);
    long l_extendedprice_x_1_l_discount_x_1_plus_l_tax = UnsafeUtil.allocVector(TajoDataTypes.Type.FLOAT8, 1024);

    long pivotVector = UnsafeUtil.alloc(2 * 1024);
    long hashResult = UnsafeUtil.allocVector(TajoDataTypes.Type.INT8, 1024);

    while(reader.nextFetch(vecRowBlock)) {
      // -------------------------------------------------------------------------------------------------------------
      // Selection
      // -------------------------------------------------------------------------------------------------------------
      int selected = SelStrLEFixedStrColVal.sel(vecRowBlock.limitedVecSize(), selVec, vecRowBlock, 6, "1998-09-01".getBytes(), 0, 0);

      // -------------------------------------------------------------------------------------------------------------
      // Projection
      // -------------------------------------------------------------------------------------------------------------

      // 1 - l_discount
      MapMinusInt4ValFloat8ColOp.map(selected, one_minus_l_discount, vecRowBlock, 1, 2, selVec);
      // l_extendedprice * (1 - l_discount)
      MapMulFloat8ColFloat8ColOp.map(selected, l_extendedprice_mul_one_minus_l_discount_ptr, vecRowBlock.getValueVecPtr(1),
          one_minus_l_discount, selVec);

      // 1 + l_tax
      MapPlusInt4ValFloat8ColOp.map(selected, one_plus_l_tax, vecRowBlock, 1, 3, selVec);

      // l_extendedprice * (1 - l_discount) * (1 + l_tax)
      MapMulFloat8ColFloat8ColOp.map(selected, l_extendedprice_x_1_l_discount_x_1_plus_l_tax, l_extendedprice_mul_one_minus_l_discount_ptr, one_plus_l_tax, selVec);

      // -------------------------------------------------------------------------------------------------------------
      // Aggregation
      // -------------------------------------------------------------------------------------------------------------
      VectorUtil.pivotCharx2(selected, vecRowBlock, pivotVector, new int[]{4,5}, selVec);
//      testPivot(vecRowBlock, pivotVector, selected, selVec);


      VecFuncMulMul3LongCol.mulmul64FixedCharVector(selected, hashResult, pivotVector, 2, 0, selVec);
//      testMapContents(vecRowBlock, hashResult, selected, selVec);


      count += selected;
      vecRowBlock.clear();
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(count + " rows (" + (readEnd - readStart) + " read msec)");
    vecRowBlock.free();
  }

  public void testPivot(VecRowBlock vecRowBlock, long pivotVector, int selected, int [] selVec) {
    long copied = pivotVector;
    for (int i = 0; i < selected; i++) {
      long offset = selVec[i] * 2;

      char b1 = (char) UnsafeUtil.getByte(copied + offset);
      char b2 = (char) UnsafeUtil.getByte(copied + offset +1);

      char storedB1 = (char) vecRowBlock.getFixedText(4, selVec[i])[0];
      char storedB2 = (char) vecRowBlock.getFixedText(5, selVec[i])[0];

      assertTrue(b1+" is different to " + storedB1, b1 == storedB1);
      assertTrue(b2+" is different to " + storedB2, b2 == storedB2);
    }
  }

  Map<Long, String> map = Maps.newHashMap();
  public void testMapContents(VecRowBlock vecRowBlock, long hashResultPtr, int selected, int [] selVec) {
    for (int i = 0; i < selected; i++) {
      int idx = selVec[i];
      long hash = UnsafeUtil.getLong(hashResultPtr, idx);
      if (map.containsKey(hash)) {
        System.out.println(new String(vecRowBlock.getFixedText(4, idx)) + "," + new String(vecRowBlock.getFixedText(5, idx)));
      } else {
        map.put(hash, new String(vecRowBlock.getFixedText(4, idx)) + "," + new String(vecRowBlock.getFixedText(5, idx)));
      }
    }
  }

  @Test
  public void testHash() {
    HashFunction hashFunc = Hashing.murmur3_128(37);
    System.out.println(hashFunc.hashString(new String("a")).asLong());
  }

  public static FileFragment createFileFragment(String name, Path path) throws IOException {
    FileSystem localFS = FileSystem.getLocal(conf);
    long length = localFS.getLength(path);
    FileFragment ff = new FileFragment(name, path, 0, length);
    return ff;
  }

  @Test
  public void testReadParquetMockup() throws IOException {
    Configuration conf = new Configuration();

    KeyValueSet KeyValueSet = StorageUtil.newPhysicalProperties(CatalogProtos.StoreType.PARQUET);
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.PARQUET, KeyValueSet);
    meta.putOption(ParquetOutputFormat.COMPRESSION, CompressionCodecName.UNCOMPRESSED.name());

    Schema projected = new Schema();
    projected.addColumn("l_orderkey", TajoDataTypes.Type.INT4, 0);
    projected.addColumn("l_partkey", TajoDataTypes.Type.INT4, 1);


    Path path = new Path("file:///home/hyunsik/experiment/data/lineitem.parquet");
    VecRowBlock vecRowBlock = new VecRowBlock(LINEITEM, 1024);
    VecRowParquetReader reader = new VecRowParquetReader(path, LINEITEM, projected);
    long readStart = System.currentTimeMillis();

    int rowIdx = 0;
    while(reader.nextFetch(vecRowBlock)) {
      for (int vectorId = 0; vectorId < vecRowBlock.maxVecSize(); vectorId++) {
        assertTrue(vecRowBlock.getInt4(0, vectorId) > 0);
        assertTrue(vecRowBlock.getInt4(1, vectorId) > 0);
        //assertEquals(rowId % 2, vecRowBlock.getBool(0, vectorId));
        //assertTrue(1 == vecRowBlock.getInt2(1, vectorId));
        //assertEquals(rowId, vecRowBlock.getInt4(2, vectorId));
//        assertEquals(rowId, vecRowBlock.getInt8(3, vectorId));
        //assertTrue(rowId == vecRowBlock.getFloat4(4, vectorId));
        //assertTrue(((double)rowId) == vecRowBlock.getFloat8(5, vectorId));
        //assertEquals("colabcdefghijklmnopqrstu1", (vecRowBlock.getString(6, vectorId)));
        //assertEquals("colabcdefghijklmnopqrstu2", (vecRowBlock.getString(7, vectorId)));
        rowIdx++;
      }
      vecRowBlock.clear();
    }
    long readEnd = System.currentTimeMillis();
    System.out.println(rowIdx + " rows (" + (readEnd - readStart) + " read msec)");
    vecRowBlock.free();
  }
}