/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class OfflineClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineClusterIntegrationTest.class);

  private final File _tmpDir = new File("/tmp/OfflineClusterIntegrationTest");
  private final File _segmentDir = new File("/tmp/OfflineClusterIntegrationTest/segmentDir");
  private final File _tarDir = new File("/tmp/OfflineClusterIntegrationTest/tarDir");

  private static final int SEGMENT_COUNT = 12;

  protected void startCluster() {
    startZk();
    startController();
    startBroker();
    startServer();
  }

  protected void createTable() throws Exception {
    File schemaFile = getSchemaFile();

    // Create a table
    setUpTable(schemaFile, 1, 1);
  }

  protected void setUpTable(File schemaFile, int numBroker, int numOffline) throws Exception {
    addSchema(schemaFile, "schemaFile");
    addOfflineTable("DaysSinceEpoch", "daysSinceEpoch", -1, "", null, null, "mytable", SegmentVersion.v1);
  }

  protected void dropTable() throws Exception {
    dropOfflineTable("mytable");
  }

  @BeforeClass
  public void setUp() throws Exception {
    //Clean up
    ensureDirectoryExistsAndIsEmpty(_tmpDir);
    ensureDirectoryExistsAndIsEmpty(_segmentDir);
    ensureDirectoryExistsAndIsEmpty(_tarDir);

    // Start the cluster
    startCluster();

    // Unpack the Avro files
    final List<File> avroFiles = unpackAvroData(_tmpDir, SEGMENT_COUNT);

    createTable();

    // Load data into H2
    ExecutorService executor = Executors.newCachedThreadPool();
    setupH2AndInsertAvro(avroFiles, executor);

    // Create segments from Avro data
    buildSegmentsFromAvro(avroFiles, executor, 0, _segmentDir, _tarDir, "mytable", false, null);

    // Initialize query generator
    setupQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Set up a Helix spectator to count the number of segments that are uploaded and unlock the latch once 12 segments are online
    final CountDownLatch latch = setupSegmentCountCountDownLatch("mytable", SEGMENT_COUNT);

    // Upload the segments
    int i = 0;
    for (String segmentName : _tarDir.list()) {
      System.out.println("Uploading segment " + (i++) + " : " + segmentName);
      File file = new File(_tarDir, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, new FileInputStream(file), file.length());
    }

    // Wait for all segments to be online
    latch.await();
    TOTAL_DOCS = 115545;
    long timeInTwoMinutes = System.currentTimeMillis() + 2 * 60 * 1000L;
    long numDocs;
    while ((numDocs = getCurrentServingNumDocs("mytable")) < TOTAL_DOCS) {
      System.out.println("Current number of documents: " + numDocs);
      if (System.currentTimeMillis() < timeInTwoMinutes) {
        Thread.sleep(1000);
      } else {
        Assert.fail("Segments were not completely loaded within two minutes");
      }
    }
  }

  /**
   * Compare the results with sql results
   * @throws Exception
   */
  @Test
  public void testDistinctCountNoGroupByQuery() throws Exception {
    String query;
    String[] testColumns = new String[]{"AirTime"/* int */, "ArrDelayMinutes"/* int */, "ArrTimeBlk"/* string */, "Carrier"/* string */};
    boolean hasWhere = true;
    LOGGER.debug("========================== Test Total " + testColumns.length * 2 + " Queries ==========================");
    for (String column: testColumns) {
      for (int i = 0; i < 2; i++) {
        query = "select distinctcount(" + column + ") from 'mytable'";
        if (hasWhere) {
          query += " where DaysSinceEpoch >= 16312";
        }
        super.runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable").replace("distinctcount(", "count(distinct ")));
        LOGGER.debug("========================== End ==========================");
        hasWhere = !hasWhere;
      }
    }
  }

  /**
   * Compare the results with sql results
   * @throws Exception
   */
  @Test
  public void testDistinctCountGroupByQuery() throws Exception {
    String query;
    String[] testColumns = new String[]{"AirTime"/* int */, "ArrDelayMinutes"/* int */, "ArrTimeBlk"/* string */};
    boolean hasWhere = true;
    LOGGER.debug("========================== Test Total " + testColumns.length * 2 + " Queries ==========================");
    for (String column: testColumns) {
      for (int i = 0; i < 2; i++) {
        /**
         * Due to test codes, group by keys must appear in the select clause!
         */
        query = "select Carrier, distinctcount(" + column + ") from 'mytable'";
        if (hasWhere) {
          query += " where DaysSinceEpoch >= 16312";
        }
        query += " group by Carrier";
        super.runQuery(query, Collections.singletonList(query.replace("'mytable'", "mytable").replace("distinctcount(", "count(distinct ")));
        LOGGER.debug("========================== End ==========================");
        hasWhere = !hasWhere;
      }
    }
  }

  /**
   * Compare HLL results with accurate distinct counting results
   * @throws Exception
   */
  @Test
  public void testDistinctCountHLLNoGroupByQuery() throws Exception {
    testApproximationQuery(
        new String[]{"distinctcount", "distinctcounthll"},
        new String[]{"AirTime"/* int */, "ArrDelayMinutes"/* int */, "ArrTimeBlk"/* string */, "Carrier"/* string */},
        null,
        TestUtils.hllEstimationThreshold);
  }

  /**
   * Compare HLL results with accurate distinct counting results
   * @throws Exception
   */
  @Test
  public void testDistinctCountHLLGroupByQuery() throws Exception {
    testApproximationQuery(
        new String[]{"distinctcount", "distinctcounthll"},
        new String[]{"AirTime"/* int */, "ArrDelayMinutes"/* int */, "ArrTimeBlk"/* string */},
        "Carrier",
        TestUtils.hllEstimationThreshold);
  }

  @Test
  public void testQuantileNoGroupByQuery() throws Exception {
    testApproximationQuery(
        new String[]{"percentile50", "percentileest50"},
        new String[]{"AirTime"/* int */, "ArrTime"/* int */},
        null,
        TestUtils.digestEstimationThreshold);
  }

  @Test
  public void testQuantileGroupByQuery() throws Exception {
    testApproximationQuery(
        new String[]{"percentile50", "percentileest50"},
        new String[]{"AirTime"/* int */, "ArrTime"/* int */},
        "Carrier",
        TestUtils.digestEstimationThreshold);
  }


  /**
   *
   * @param functionNames: accurate function comes first
   * @param testColumns
   * @param groupByColumn
   * @param precision
   * @throws Exception
   */
  private void testApproximationQuery(String[] functionNames, String[] testColumns, String groupByColumn, double precision) throws Exception {
    String query;
    boolean hasWhere = true;
    LOGGER.debug("========================== Test Total " + testColumns.length * 2 + " Queries ==========================");
    for (String column: testColumns) {
      for (int i = 0; i < 2; i++) {
        query = "select " + functionNames[0] + "(" + column + ") from 'mytable'";
        if (hasWhere) {
          query += " where DaysSinceEpoch >= 16312";
        }
        if (groupByColumn != null) {
          query += " group by " + groupByColumn;
          JSONArray accurate = getGroupByArrayFromJSONAggregationResults(postQuery(query));
          query = query.replace(functionNames[0], functionNames[1]);
          JSONArray estimate = getGroupByArrayFromJSONAggregationResults(postQuery(query));
          TestUtils.assertJSONArrayApproximation(estimate, accurate, precision);
        } else {
          double accurate = Double.parseDouble(getSingleStringValueFromJSONAggregationResults(postQuery(query)));
          query = query.replace(functionNames[0], functionNames[1]);
          double estimate = Double.parseDouble(getSingleStringValueFromJSONAggregationResults(postQuery(query)));
          TestUtils.assertApproximation(estimate, accurate, precision);
          //
        }
        LOGGER.debug("========================== End ==========================");
        hasWhere = !hasWhere;
      }
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    stopServer();
    try {
      stopZk();
    } catch (Exception e) {
      // Swallow ZK Exceptions.
    }
    FileUtils.deleteDirectory(_tmpDir);
  }
}
