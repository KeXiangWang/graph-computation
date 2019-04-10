package ed.inf.grape.graph;

import java.io.Serializable;

public class Node implements Serializable {

	private int id; // node ID
	private int attribute; // attribute of the node
	private Edge firstin; // first in node
	private Edge firstout; // first out node

	public Node(int id) {
		this.id = id;
	}

	public Node(int id, int attr) {
		this.id = id;
		this.attribute = attr;
	}

	public Node(int id, int attribute, Edge firstin, Edge firstout) {
		this.id = id;
		this.attribute = attribute;
		this.firstin = firstin;
		this.firstout = firstout;
	}

	public int GetID() {
		return this.id;
	}

	public int GetAttribute() {
		return this.attribute;
	}

	public Edge GetFirstIn() {
		return this.firstin;
	}

	public Edge GetFirstOut() {
		return this.firstout;
	}

	public void SetID(int ID) {
		this.id = ID;
	}

	public void SetAttribute(int attribute) {
		this.attribute = attribute;
	}

	public void SetFirstIn(Edge firstin) {
		this.firstin = firstin;
	}

	public void SetFirstOut(Edge firstout) {
		this.firstout = firstout;
	}

	// ��֤��equals��������ȷ��
	public boolean equals(Object other) {
		if (this == other) // �ȼ���Ƿ����Է��ԣ���Ƚ�other�Ƿ�Ϊ�ա�����Ч�ʸ�
			return true;
		if (other == null)
			return false;
		if (!(other instanceof Node))
			return false;

		final Node v = (Node) other;
		if (this.GetID() != v.GetID()) {
			return false;
		}
		// if(!this.GetAttribute().equals(v.GetAttribute())){
		// return false;
		// }
		return true;
	}

	public int hashCode() { // hashCode��Ҫ���������hashϵͳ�Ĳ�ѯЧ�ʡ���hashCode�в������κβ���ʱ������ֱ�����䷵��
							// һ���������߲�������д��
		// uniq id
		int result = String.valueOf(this.GetID()).hashCode();
		// result = 29 * result +this.GetAttribute().hashCode();
		return result;
	}

}
