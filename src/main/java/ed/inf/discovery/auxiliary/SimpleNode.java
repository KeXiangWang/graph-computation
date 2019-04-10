package ed.inf.discovery.auxiliary;

import java.io.Serializable;

public class SimpleNode implements Serializable {

	private static final long serialVersionUID = 2655808949983421767L;

	public int nodeID;
	public int attribute;
	public int hop;

	public SimpleNode(int nodeID, int attribute, int hop) {
		super();
		this.nodeID = nodeID;
		this.attribute = attribute;
		this.hop = hop;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + attribute;
		result = prime * result + hop;
		result = prime * result + nodeID;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimpleNode other = (SimpleNode) obj;
		if (attribute != other.attribute)
			return false;
		if (hop != other.hop)
			return false;
		if (nodeID != other.nodeID)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[NodeID:" + nodeID + ", a=" + attribute + ", h=" + hop + "]";
	}

}
