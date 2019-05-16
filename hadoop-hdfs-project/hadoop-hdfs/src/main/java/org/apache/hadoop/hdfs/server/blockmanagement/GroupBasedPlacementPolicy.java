package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

public class GroupBasedPlacementPolicy extends BlockPlacementPolicyDefault {

  private AtomicInteger blocksNoGroupLocal = new AtomicInteger(0);

  @Override
  protected DatanodeStorageInfo chooseRandom(String scope, 
      Set<Node> excludedNodes, long blocksize,
      int maxNodesPerRack, List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, String storageGroup) throws NotEnoughReplicasException {
    // replace scope with groupScope
    String groupScope = scope + NodeBase.PATH_SEPARATOR_STR + storageGroup;
    return chooseRandom(1, groupScope, excludedNodes, blocksize, 
        maxNodesPerRack, results, avoidStaleNodes, storageTypes, storageGroup);
  }

  @Override
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes, String storageGroup,
      boolean fallbackToLocalRack) throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes, storageGroup);
    }
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine)) { // was not in the excluded list
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes.entrySet().iterator();
            iter.hasNext();) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (DatanodeStorageInfo localStorage
              : DFSUtil.shuffle(localDatanode.getStorageInfos())) {
            StorageType type = entry.getKey();
            if (addIfIsGoodTarget(localStorage, excludedNodes, blocksize, maxNodesPerRack,
                false, results, avoidStaleNodes, type, storageGroup) >= 0) {
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              return localStorage;
            }
          }
        }
      }
    }
    if (!fallbackToLocalRack) {
      return null;
    }
    return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageTypes, storageGroup);
  }

  @Override
  protected Node chooseTarget(int numOfReplicas, Node writer, final Set<Node> excludedNodes,
      final long blocksize, final int maxNodesPerRack, final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes, final BlockStoragePolicy storagePolicy,
      final EnumSet<StorageType> unavailableStorages, final String storageGroup,
      final boolean newBlock) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return (writer instanceof DatanodeDescriptor) ? writer : null;
    }
    final int numOfResults = results.size();
    final int totalReplicasExpected = numOfReplicas + numOfResults;
    if ((writer == null || !(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0).getDatanodeDescriptor();
    }

    // Keep a copy of original excludedNodes
    final Set<Node> oldExcludedNodes = new HashSet<Node>(excludedNodes);

    // choose storage types; use fallbacks for unavailable storages
    final List<StorageType> requiredStorageTypes = storagePolicy
        .chooseStorageTypes((short) totalReplicasExpected,
            DatanodeStorageInfo.toStorageTypes(results), unavailableStorages, newBlock);
    final EnumMap<StorageType, Integer> storageTypes = getRequiredStorageTypes(requiredStorageTypes);
    if (LOG.isTraceEnabled()) {
      LOG.trace("storageTypes=" + storageTypes);
    }

    try {
      if ((numOfReplicas = requiredStorageTypes.size()) == 0) {
        throw new NotEnoughReplicasException("All required storage types are unavailable: "
            + " unavailableStorages=" + unavailableStorages + ", storagePolicy=" + storagePolicy);
      }
      if (numOfResults == 0) {
        DatanodeStorageInfo storageInfo = chooseLocalStorage(writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes, storageGroup, true);
        if (storageInfo == null) {
          throw new NotEnoughReplicasException("All required storage types are unavailable: "
              + " unavailableStorages=" + unavailableStorages + ", storagePolicy=" + storagePolicy);
        }
        writer = storageInfo.getDatanodeDescriptor();
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      // replace scope with groupScope
      String groupScope = NodeBase.PATH_SEPARATOR_STR + storageGroup;
      chooseRandom(numOfReplicas, groupScope, excludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes, storageGroup);
    } catch (NotEnoughReplicasException e) {
      final String message = "Failed to place enough replicas, still in need of "
          + (totalReplicasExpected - results.size()) + " to reach " + totalReplicasExpected
          + " (unavailableStorages=" + unavailableStorages + ", storagePolicy=" + storagePolicy
          + ", newBlock=" + newBlock + ")";
      if (LOG.isTraceEnabled()) {
        LOG.trace(message, e);
      } else {
        LOG.warn(message + " " + e.getMessage());
      }
      if (avoidStaleNodes) {
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize, maxNodesPerRack,
            results, false, storagePolicy, unavailableStorages, storageGroup, newBlock);
      }
      boolean retry = false;
      for (StorageType type : storageTypes.keySet()) {
        if (!unavailableStorages.contains(type)) {
          unavailableStorages.add(type);
          retry = true;
        }
      }
      if (retry) {
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize, maxNodesPerRack,
            results, false, storagePolicy, unavailableStorages, storageGroup, newBlock);
      } else {
        blocksNoGroupLocal.incrementAndGet();
        StringBuilder excluded = new StringBuilder();
        for(Node node : excludedNodes){
          excluded.append(node.getName()).append(";");
        }

        StringBuilder result = new StringBuilder();
        for(DatanodeStorageInfo info : results){
          result.append(info.getDatanodeDescriptor().getHostName()).append(";");
        }
        LOG.warn("target Group " + storageGroup + " can't place enough replicas! writerName: " 
            + writer.getName() + ", excludedNode:" + excluded.toString()
            + ", results:" + result.toString());
      }
    }
    return writer;
  }

  @Override
  protected int getBlocksNoGroupLocal() {
    return blocksNoGroupLocal.get();
  }
}
