package org.apache.hadoop.net;

public class GroupBasedNetworkTopology extends NetworkTopology {

	@Override
	public int getNumOfRacks() {
		return 1;
	}

}
