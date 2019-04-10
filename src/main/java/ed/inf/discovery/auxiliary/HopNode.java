package ed.inf.discovery.auxiliary;

import ed.inf.grape.graph.Node;

public class HopNode implements Comparable<HopNode> {
	public Node node;
	public int hop;

	public HopNode(Node node, int hop) {
		this.node = node;
		this.hop = hop;
	}

	@Override
	public int compareTo(HopNode o) {
		// TODO Auto-generated method stub
		return o.hop - this.hop;
	}
}
