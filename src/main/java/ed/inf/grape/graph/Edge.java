package ed.inf.grape.graph;

import java.io.Serializable;

public class Edge implements Serializable {

	private Node fromNode; // ��ʼ�ڵ� (��β)
	private Node toNode; // ��ֹ�ڵ� (��ͷ)
	private Edge hlink; // ָ��ͷ��ͬ����һ����
	private Edge tlink; // ָ��β��ͬ����һ����

	/**
	 * CONSTRUCTOR
	 * 
	 * @param fromNode
	 * @param toNode
	 * @param hlink
	 * @param tlink
	 */
	public Edge() {
	}

	public Edge(Node fromNode, Node toNode, Edge hlink, Edge tlink) {
		this.fromNode = fromNode;
		this.toNode = toNode;
		this.hlink = hlink;
		this.tlink = tlink;
	}

	public Node GetFromNode() {
		return this.fromNode;
	}

	public Node GetToNode() {
		return this.toNode;
	}

	public Edge GetHLink() {
		return this.hlink;
	}

	public Edge GetTLink() {
		return this.tlink;
	}

	public void SetFromNode(Node fnode) {
		this.fromNode = fnode;
	}

	public void SetToNode(Node tnode) {
		this.toNode = tnode;
	}

	public void SetHLink(Edge hlink) {
		this.hlink = hlink;
	}

	public void SetTLink(Edge tlink) {
		this.tlink = tlink;
	}

	// ��֤��equals��������ȷ��
	public boolean equals(Object other) {
		if (this == other) // �ȼ���Ƿ����Է��ԣ���Ƚ�other�Ƿ�Ϊ�ա�����Ч�ʸ�
			return true;
		if (other == null)
			return false;
		if (!(other instanceof Edge))
			return false;

		final Edge e = (Edge) other;
		if (!this.fromNode.equals(e.GetFromNode())
				|| !this.toNode.equals(e.GetToNode())) {
			return false;
		}
		return true;
	}

	public int hashcode() {
		int result = String.valueOf(this.fromNode).hashCode()
				+ String.valueOf(this.toNode).hashCode();
		return result;
	}
}
