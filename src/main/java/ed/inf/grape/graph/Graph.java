package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import ed.inf.grape.util.Dev;

public class Graph implements Serializable {

	private HashMap<Integer, Node> NodeSet; // maintains the whole set of nodes
											// in the
	// Graph.
	private int nodeSize; // amount of nodes
	private int edgeSize; // amount of edges

	/**
	 * constructor
	 */
	public Graph() {
		this.NodeSet = new HashMap<Integer, Node>();
	}

	/**
	 * another constructor
	 * 
	 * @param nodeSize
	 *            : amount of nodes
	 * @param edgeSize
	 *            : amount of edges
	 */
	public Graph(int nodeSize, int edgeSize) {
		this.nodeSize = nodeSize;
		this.edgeSize = edgeSize;
		this.NodeSet = new HashMap<Integer, Node>();
	}

	/**
	 * another constructor
	 * 
	 * @param NodeSet
	 *            : a set of nodes
	 * @param nodeSize
	 *            : amount of nodes
	 * @param edgeSize
	 *            : amount of edges
	 */
	public Graph(HashMap<Integer, Node> NodeSet, int nodeSize, int edgeSize) {
		this.NodeSet = NodeSet;
		this.nodeSize = nodeSize;
		this.edgeSize = edgeSize;
	}

	public HashMap<Integer, Node> GetNodeSet() {
		return this.NodeSet;
	}

	public int GetNodeSize() {
		return this.nodeSize;
	}

	public int GetEdgeSize() {
		return this.edgeSize;
	}

	public void SetNodeSet(HashMap<Integer, Node> NodeSet) {
		this.NodeSet = NodeSet;
	}

	public void SetNodeSize(int nodeSize) {
		this.nodeSize = nodeSize;
	}

	public void SetEdgeSize(int edgeSize) {
		this.edgeSize = edgeSize;
	}

	/**
	 * insert a new node in the graph.
	 * 
	 * @param n
	 */
	public void InsNode(Node n) {
		this.NodeSet.put(n.GetID(), n);
		this.nodeSize++;
	}

	/**
	 * delete a node and all the edges connecting to the node
	 *
	 * 删点操作等同于, 1) 删除所有与该点连接的边, 2) 然后再删除点.
	 * 
	 * @param n
	 */
	public void DelNode(Node n) {
		this.NodeSet.remove(n.GetID()); // remove the node from node set.
		this.nodeSize--;
		// 需要调整this.edgeSize
		// process n's parents
		Edge ep = n.GetFirstIn();
		if (ep != null) {
			for (Edge e = ep; e != null; e = e.GetHLink()) {
				Node fv = e.GetFromNode();
				for (Edge e1 = fv.GetFirstOut(); e1 != null; e1 = e1.GetTLink()) {
					Edge e2 = e1.GetTLink(); // e2.GetToNode() 有可能是n
					Node cfv = e1.GetToNode();
					if (cfv.equals(n)) { // e1.firstout 指向了被删除的边
						fv.SetFirstOut(e2); // 如果e1.tonode = n,
											// 则设置fv的firstout为e2
						break;
					} else {
						Node cfv2 = e2.GetToNode();
						if (cfv2.equals(n)) {
							e1.SetTLink(e2.GetTLink()); // e2是e1的邻边,
														// 如果e2.tonode = n
														// (说明e2为被删除的边),
														// 则设置e1的tlink为e2的tlink
							break;
						}
					}
				}
			}
		}

		// process n's children
		ep = n.GetFirstOut();
		if (ep != null) {
			for (Edge e = ep; e != null; e = e.GetTLink()) {
				Node tv = e.GetToNode();
				for (Edge e1 = tv.GetFirstIn(); e1 != null; e1 = e1.GetHLink()) {
					Edge e2 = e1.GetHLink();
					Node ptv = e1.GetFromNode();
					if (ptv.equals(n)) {
						tv.SetFirstIn(e2);
						break;
					} else {
						Node ptv2 = e2.GetFromNode();
						if (ptv2.equals(n)) {
							e1.SetHLink(e2.GetHLink());
							break;
						}
					}
				}
			}
		}
	}

	// public void DelNode(Node n){
	// Queue<Edge> q = new LinkedList<Edge>();
	// Edge e = n.GetFirstOut(); // points to the child node of n.
	// HashSet<Edge> visited = new HashSet<Edge>();
	// q.add(e);
	// visited.add(e);
	// while(!q.isEmpty()){
	// Edge ep = q.poll(); // the edge which connects n and its child node
	// Node cn = ep.GetToNode(); // child node of n
	// Edge eh = ep.GetHLink(); // the edge that has the same head node as ep
	// Edge et = ep.GetTLink(); // the edge that has the same tail node as ep
	// (ep's tail node is n)
	// cn.SetFirstIn(eh); // changes on the node
	//
	// Edge ne = n.GetFirstIn();
	//
	//
	//
	// if(!!visited.contains(et) && et!=null){ // if et is not visited, then
	// q.add(et);
	// visited.add(et);
	// }
	// // ���м�Ͽ��Ĳ��ֽ���
	//
	// }
	//
	// q.clear();
	// visited.clear();
	// e = n.GetFirstIn();
	// q.add(e);
	// while(!q.isEmpty()){
	// Edge ep = q.poll();
	// Node fn = ep.GetFromNode();
	// Edge ee = ep.GetTLink();
	// fn.SetFirstOut(ee); // changes on the node
	// if(!e.equals(ee)){
	// q.add(ee);
	// }
	// }
	// }

	/**
	 * this procedure inserts a new edge into graph.
	 * 
	 * @param fv
	 *            : from node
	 * @param tv
	 *            : to node
	 */
	public void InsEdge(Node fv, Node tv, Integer label) {
		// DONE set label
		Edge e = new Edge();
		e.SetFromNode(fv);
		e.SetToNode(tv);
		Edge hlink = tv.GetFirstIn();
		Edge tlink = fv.GetFirstOut();
		e.SetHLink(hlink);
		e.SetTLink(tlink);
		fv.SetFirstOut(e);
		tv.SetFirstIn(e);
		this.edgeSize++;
	}

	/**
	 * this procedure inserts a new edge into graph.
	 *
	 * @param fv
	 *            : from node
	 * @param tv
	 *            : to node
	 */
	public void InsEdge(Node fv, Node tv) {
		// DONE set label
		Edge e = new Edge();
		e.SetFromNode(fv);
		e.SetToNode(tv);
		Edge hlink = tv.GetFirstIn();
		Edge tlink = fv.GetFirstOut();
		e.SetHLink(hlink);
		e.SetTLink(tlink);
		fv.SetFirstOut(e);
		tv.SetFirstIn(e);
		this.edgeSize++;
	}

	/**
	 * remove an edge with fv as tail node and tv as head node.
	 */
	public void DelEdge(Node fv, Node tv) {
		// ��Ҫ����this.edgeSize
		// �޸�fv�ĳ�������
		Edge e1 = fv.GetFirstOut();
		if (e1.GetToNode().equals(tv)) { // ���fv��firstout��ǡ���Ǳ�ɾ���ı�
			fv.SetFirstOut(e1.GetTLink()); // e1��tlink����Ϊnull
		} else {
			for (Edge e = e1; e != null; e = e.GetTLink()) { // ������e����ͬtail
																// node�ı�
				Edge ne = e.GetTLink(); // ne ����Ϊnull
				if (ne != null) {
					if (ne.GetToNode().equals(tv)) { // ne �Ǽ���ɾ���ı�,
														// ��ô��Ҫ��e.tlink����Ϊne.tlink
						e.SetTLink(ne.GetTLink());
						break;
					}
				}
			}
		}

		// �޸�tv���뻡����
		e1 = tv.GetFirstIn();
		if (e1.GetFromNode().equals(fv)) { // ���tv��firstin��ǡ���Ǳ�ɾ���ı�
			tv.SetFirstIn(e1.GetHLink()); // e1.hlink����Ϊnull
		} else {
			for (Edge e = e1; e != null; e = e.GetHLink()) {
				Edge ne = e.GetHLink(); // ne ����Ϊnull
				if (ne != null) {
					if (ne.GetFromNode().equals(fv)) { // ne
														// �Ǽ���ɾ���ıߣ���ô��Ҫ��e.hlink����Ϊne.hlink
						e.SetHLink(ne.GetHLink());
						break;
					}
				}
			}
		}
	}

	/**
	 * finds a node with the specified node id
	 * 
	 * @param id
	 */
	public Node FindNode(int id) {
		// for (Node n : this.NodeSet) {
		// if (n.GetID() == id) {
		// return n;
		// }
		// }

		return this.NodeSet.get(id);
		// return null;
	}

	/**
	 * finds a node randomly
	 *
	 * @return
	 */
	public Node FindANode() {

		List<Integer> list = new LinkedList<Integer>(this.NodeSet.keySet());
		int nodeId = list.get((int) (Math.random() * list.size()));
		return this.NodeSet.get(nodeId);
	}

	/**
	 * get children nodes of n
	 * 
	 * @param n
	 * @return
	 */
	public HashSet<Node> GetChildren(Node n) {
		HashSet<Node> cSet = new HashSet<Node>();
		Queue<Edge> q = new LinkedList<Edge>();
		Edge e = n.GetFirstOut();
		if (e != null) {
			q.add(e);
		}

		while (!q.isEmpty()) {
			Edge ee = q.poll();
			cSet.add(ee.GetToNode());
			Edge ne = ee.GetTLink();
			if (ne != null) {
				q.add(ne);
			}
		}
		return cSet;
	}

	/**
	 * get parent nodes of n
	 * 
	 * @param n
	 * @return
	 */
	public HashSet<Node> GetParents(Node n) {
		HashSet<Node> pSet = new HashSet<Node>();
		Edge e1 = n.GetFirstIn();
		for (Edge e = e1; e != null; e = e.GetHLink()) {
			Node parent = e.GetFromNode();
			pSet.add(parent);
		}
		return pSet;
	}

	/**
	 * displays graph structure using adjacency list.
	 */
	public void Display() {
		System.out.println("The graph has the following structure: ");
		for (Node n : this.NodeSet.values()) {
			Edge e1 = n.GetFirstOut();
			String s = "";
			if (e1 != null) {
				for (Edge e = e1; e != null; e = e.GetTLink()) {
					s = s + ", " + e.GetToNode().GetID();
				}
				if (!s.equals(""))
					s = s.substring(2);
			}
			System.out.println(n.GetID() + ", label: " + n.GetAttribute() + ", links: " + s);
		}
	}

	// test how large a graph can be loaded by using Orthogonal list.
	public void Init() {
		for (int i = 0; i < 4; i++) {
			Node n = new Node(i);
			this.InsNode(n);
		}
		Node n0 = this.FindNode(0);
		Node n1 = this.FindNode(1);
		Node n2 = this.FindNode(2);
		Node n3 = this.FindNode(3);

		this.InsEdge(n0, n1);
		this.InsEdge(n0, n2);
		this.InsEdge(n0, n3);
		this.InsEdge(n1, n2);
		this.InsEdge(n3, n2);
	}

	public void RanGraphGen(int vsize, int esize) {
		this.SetNodeSize(vsize);
		this.SetEdgeSize(esize);

		// add nodes
		for (int i = 0; i < vsize; i++) {
			Node n = new Node(i, 0, null, null);
			this.NodeSet.put(n.GetID(), n);
		}
		// add edges
		for (int i = 0; i < esize; i++) {
			Node fn = this.NodeSet.get((int) (Math.random() * vsize));
			Node tn = this.NodeSet.get((int) (Math.random() * vsize));
			while (fn.equals(tn)) {
				fn = this.NodeSet.get((int) (Math.random() * vsize));
				tn = this.NodeSet.get((int) (Math.random() * vsize));
			}
			Edge e = new Edge();
			e.SetFromNode(fn);
			e.SetToNode(tn);
			Edge hlink = tn.GetFirstIn();
			Edge tlink = fn.GetFirstOut();
			e.SetHLink(hlink);
			e.SetTLink(tlink);
			fn.SetFirstOut(e);
			tn.SetFirstIn(e);
		}
	}

	public static void main(String[] args) {

		Graph G = new Graph(4, 5);
		G.Init();
		G.Display();
		// Node fv = G.FindNode(0);
		// Node tv = G.FindNode(2);
		// G.DelEdge(fv, tv);
		// Node n = G.FindNode(0);
		// G.DelNode(n);
		// G.Display();

		// G.NodeSet = new LinkedList<Node>();
		System.out.println(Dev.currentRuntimeState());
		G.RanGraphGen(1000000, 5000000);
		System.out.println(Dev.currentRuntimeState());
		// for (Node n : G.NodeSet) {
		// HashSet<Node> cSet = G.GetChildren(n);
		// String cStr = "";
		// for (Node c : cSet) {
		// cStr = cStr + ", " + String.valueOf(c.GetID());
		// }
		// if (!cStr.equals("")) {
		// cStr = cStr.substring(1);
		// }
		// System.out.println(n.GetID() + "    " + cStr);
		// }
	}

}
