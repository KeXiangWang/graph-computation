package ed.inf.discovery.auxiliary;

public class FreqEdge {
	public int fNodeLabel;
	public int tNodeLabel;

	public FreqEdge(String edge) {
		String[] nodes = edge.split("-");
		this.fNodeLabel = Integer.parseInt(nodes[0]);
		this.tNodeLabel = Integer.parseInt(nodes[1]);
	}

	public FreqEdge(int fnode, int tnode) {
		this.fNodeLabel = fnode;
		this.tNodeLabel = tnode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + fNodeLabel;
		result = prime * result + tNodeLabel;
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
		FreqEdge other = (FreqEdge) obj;
		if (fNodeLabel != other.fNodeLabel)
			return false;
		if (tNodeLabel != other.tNodeLabel)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return fNodeLabel + "-" + tNodeLabel;
	}

}
