/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.GroupBasedNetworkTopology;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestReplicationPolicyWithGroup {
  {
    ((Log4JLogger)BlockPlacementPolicy.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 8;
  private static NetworkTopology cluster;
  private static NameNode namenode;
  private static BlockPlacementPolicy replicator;
  private static final String filename = "/dummyfile.txt";
  private static DatanodeDescriptor[] dataNodes;
  private static DatanodeStorageInfo[] storages;

  @Rule
  public ExpectedException exception = ExpectedException.none();
  
  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
    long capacity, long dfsUsed, long remaining, long blockPoolUsed,
    long dnCacheCapacity, long dnCacheUsed, int xceiverCount, int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/GROUP1",
        "/GROUP2",
        "/GROUP2",
        "/GROUP3",
        "/GROUP3",
        "/GROUP3",
        "/NONE",
        "/NONE"
    };
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicyWithGroup.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, false);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, false);
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY, 
        GroupBasedPlacementPolicy.class.getName());
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_IMPL_KEY, 
        GroupBasedNetworkTopology.class.getName());
    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    replicator = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    // construct network topology
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
      bm.getDatanodeManager().getHeartbeatManager().addDatanode(
          dataNodes[i]);
    }
    resetHeartbeatForStorages();
  }

  private static void resetHeartbeatForStorages() {
    for (int i=0; i < NUM_OF_DATANODES; i++) {
      updateHeartbeatWithUsage(dataNodes[i],
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    }    
  }

  private static boolean isOnSameGroup(DatanodeStorageInfo left, DatanodeStorageInfo right) {
    return isOnSameGroup(left, right.getDatanodeDescriptor());
  }

  private static boolean isOnSameGroup(DatanodeStorageInfo left, DatanodeDescriptor right) {
    return cluster.isOnSameRack(left.getDatanodeDescriptor(), right);
  }

  /**
   * In this testcase, client is dataNodes[0]. So the 1st replica should be
   * placed on dataNodes[0], and the other replicas should be placed on the target group,
   * if the target group does not have enough resources, use the default group(NONE)  
   * @throws Exception
   */
  @Test
  public void testChooseTarget1() throws Exception {
    resetHeartbeatForStorages();
    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        0L, 0L, 4, 0); // overloaded

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, "GROUP1");
    assertEquals(targets.length, 0);
    
    targets = chooseTarget(1, "GROUP1");
    assertEquals(targets.length, 1);
    assertEquals(storages[0], targets[0]); // chose local
    
    targets = chooseTarget(2, "GROUP1");
    // only one node in Group1
    assertEquals(targets.length, 1);
    assertEquals(storages[0], targets[0]);

    targets = chooseTarget(4, "GROUP3");
    // one local, three GROUP3
    assertEquals(targets.length, 4);
    assertEquals(storages[0], targets[0]);
    assertTrue(isOnSameGroup(targets[1], targets[2]) &&
        isOnSameGroup(targets[2], targets[3]));
    assertFalse(isOnSameGroup(targets[0], targets[2]));

    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
    resetHeartbeatForStorages();
  }

  /**
   * In this testcase, client is dataNodes[0], but the dataNodes[7] is
   * not allowed to be chosen. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on dataNodes[6]
   * @throws Exception
   */
  @Test
  public void testChooseTarget2() throws Exception {
    resetHeartbeatForStorages();
    Set<Node> excludedNodes;
    DatanodeStorageInfo[] targets;
    List<DatanodeStorageInfo> chosenNodes = new ArrayList<DatanodeStorageInfo>();

    excludedNodes = new HashSet<Node>();
    excludedNodes.add(dataNodes[7]); 
    targets = chooseTarget(0, chosenNodes, excludedNodes, "GROUP1");
    assertEquals(targets.length, 0);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[7]);
    targets = chooseTarget(1, chosenNodes, excludedNodes, "GROUP1");
    assertEquals(targets.length, 1);
    assertEquals(storages[0], targets[0]);

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[7]); 
    targets = chooseTarget(2, chosenNodes, excludedNodes, "NONE");
    // one local, one dataNodes[6]
    assertEquals(targets.length, 2);
    assertEquals(storages[0], targets[0]);
    assertFalse(isOnSameGroup(targets[0], targets[1]));

    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[7]); 
    // one local, one dataNodes[6], lost one
    targets = chooseTarget(3, chosenNodes, excludedNodes, "NONE");
    assertEquals(targets.length, 2);
    assertEquals(storages[0], targets[0]);
    assertFalse(isOnSameGroup(targets[0], targets[1]));
  }

  /**
   * In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
   * to be chosen. So the 1st replica should be placed on dataNodes[6] or dataNodes[7] 
   * @throws Exception
   */
  @Test
  public void testChooseTarget3() throws Exception {
    resetHeartbeatForStorages();
    // make data node 0 to be not qualified to choose
    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (HdfsConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L,
        0L, 0L, 0, 0); // no space
        
    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, "NONE");
    assertEquals(targets.length, 0);

    targets = chooseTarget(1, "NONE");
    // on datanode[6] or datanode[7]
    assertEquals(targets.length, 1);
    assertTrue(isOnSameGroup(targets[0], storages[6]));

    targets = chooseTarget(2, "NONE");
    // on datanode[6] or datanode[7]
    assertEquals(targets.length, 2);
    assertTrue(isOnSameGroup(targets[0], targets[1]));

    updateHeartbeatWithUsage(dataNodes[0],
        2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        HdfsConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0L, 0L, 0, 0);
  }

  /**
   * In this testcase, client is is a node outside of file system.
   * the 1st and 2nd replica can be placed on GROUP2. 
   * the 3rd replica should be placed on the default group
   * @throws Exception
   */
  @Test
  public void testChooseTarget4() throws Exception {
    resetHeartbeatForStorages();
    DatanodeDescriptor writerDesc =
      DFSTestUtil.getDatanodeDescriptor("9.9.9.9", "/CLIENT");

    DatanodeStorageInfo[] targets;
    targets = chooseTarget(0, writerDesc, "GROUP2");
    assertEquals(targets.length, 0);

    targets = chooseTarget(1, writerDesc, "GROUP2");
    assertEquals(targets.length, 1);

    targets = chooseTarget(2, writerDesc, "GROUP2");
    assertEquals(targets.length, 2);
    assertTrue(isOnSameGroup(targets[0], targets[1]));

    targets = chooseTarget(3, writerDesc, "GROUP2");
    // 1st and 2nd on GROUP2
    assertEquals(targets.length, 2);
    assertTrue(isOnSameGroup(targets[0], targets[1]));
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas, String group) {
    return chooseTarget(numOfReplicas, dataNodes[0], group);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, String group) {
    return chooseTarget(numOfReplicas, writer,
        new ArrayList<DatanodeStorageInfo>(), group);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeStorageInfo> chosenNodes, String group) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, null, group);
  }

  private static DatanodeStorageInfo[] chooseTarget(
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeStorageInfo> chosenNodes,
      Set<Node> excludedNodes, String group) {
    return replicator.chooseTarget(filename, numOfReplicas, writer, chosenNodes,
        false, excludedNodes, BLOCK_SIZE, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY,
        group);
  }

  private static DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
      List<DatanodeStorageInfo> chosenNodes, Set<Node> excludedNodes, String group) {
    return chooseTarget(numOfReplicas, dataNodes[0], chosenNodes, excludedNodes, group);
  }
}
