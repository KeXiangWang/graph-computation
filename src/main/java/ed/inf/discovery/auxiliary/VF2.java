package ed.inf.discovery.auxiliary;

import java.util.HashSet;
import java.util.Vector;

import ed.inf.grape.graph.Graph;
import ed.inf.grape.graph.Node;

public class VF2 {

	private Graph G;
	private Graph Q;
	public int num;
	public boolean flag = false;

	/**
	 * constructor
	 */
	public VF2(Graph G, Graph Q) {
		this.G = G;
		this.Q = Q;
		this.num = 0;
	}

	public Vector<Node[]> findIso(Vector<Node> in_n, Vector<Node> in_m, Vector<Node> out_n,
			Vector<Node> out_m, Vector<Node> core_n, Vector<Node> core_m) {
		Vector<Node[]> canIso = new Vector<Node[]>();
		if (in_n.size() > 0 && in_m.size() > 0) {
			Node n2 = in_m.firstElement();
			int n2label = n2.GetAttribute();
			for (Node n1 : in_n) {
				int n1label = n1.GetAttribute();
				if (n2label == n1label) {
					Node[] pair = { n1, n2 };
					canIso.add(pair);
				}
			}
		} else if (out_n.size() > 0 && out_m.size() > 0) {
			Node n2 = out_m.firstElement();
			int n2label = n2.GetAttribute();
			for (Node n1 : out_n) {
				int n1label = n1.GetAttribute();
				if (n2label == n1label) {
					Node[] pair = { n1, n2 };
					canIso.add(pair);
				}
			}
		} else if (in_n.size() == 0 && in_m.size() == 0 && out_n.size() == 0 && out_m.size() == 0) { // used
																										// for
																										// initialisation
			Node u = this.Q.FindANode();
			for (Node v : this.G.GetNodeSet().values()) {
				if (u.GetAttribute() == v.GetAttribute()) {
					Node[] pair = { v, u };
					canIso.add(pair);
				}
			}
		}
		// do not consider following case temporarily
		// else{ // in presence of not connected graph
		// }
		return canIso;
	}

	/**
	 * @param n
	 *            : in NG, shall be added in the partial matching
	 * @param m
	 *            : in MG, shall be added in the partial matcing
	 * @param core_n
	 *            : partial matching, including nodes in NG
	 * @param core_m
	 *            : partial matching, including nodes in MG
	 * @param NG
	 *            : data graph
	 * @param MG
	 *            : pattern graph
	 * @return
	 */
	public boolean verifyR_pred(Node n, Node m, Vector<Node> core_n, Vector<Node> core_m, Graph NG,
			Graph MG) {
		boolean R_pred = true;
		HashSet<Node> parentN = NG.GetParents(n);
		HashSet<Node> parentM = MG.GetParents(m);
		parentN.retainAll(core_n);
		parentM.retainAll(core_m);
		if ((parentN.isEmpty() && !parentM.isEmpty()) || (!parentN.isEmpty() && parentM.isEmpty())) {
			return false;
		} else {
			for (Node pn : parentN) {
				boolean flag = false;
				int idx = core_n.indexOf(pn);
				Node pm = core_m.get(idx);
				if (pm != null && parentM.contains(pm)) {
					flag = true;
				}
				if (!flag) { // parentM does not contain corresponding node
					R_pred = false;
					break;
				}
			}

			if (!R_pred) {
				return false;
			}

			// ¶Ô³ÆÐÔ
			for (Node pm : parentM) {
				boolean flag = false;
				int idx = core_m.indexOf(pm);
				Node pn = core_n.get(idx);
				if (pn != null && parentN.contains(pn)) {
					flag = true;
				}
				if (!flag) { // parentN does not contain corresponding node
					R_pred = false;
					break;
				}
			}
		}
		return true;
	}

	public boolean verifyR_succ(Node n, Node m, Vector<Node> core_n, Vector<Node> core_m, Graph NG,
			Graph MG) {
		boolean R_succ = true;
		HashSet<Node> childrenN = NG.GetChildren(n);
		HashSet<Node> childrenM = MG.GetChildren(m);
		childrenN.retainAll(core_n);
		childrenM.retainAll(core_m);
		if ((childrenN.isEmpty() && !childrenM.isEmpty())
				|| (!childrenN.isEmpty() && childrenM.isEmpty())) {
			return false;
		} else {
			for (Node cn : childrenN) {
				boolean flag = false;
				int idx = core_n.indexOf(cn);
				Node pm = core_m.get(idx);
				if (pm != null && childrenM.contains(pm)) {
					flag = true;
				}
				if (!flag) { // parentM does not contain corresponding node
					R_succ = false;
					break;
				}
			}

			if (!R_succ) {
				return false;
			}

			// ¶Ô³ÆÐÔ
			for (Node pm : childrenM) {
				boolean flag = false;
				int idx = core_m.indexOf(pm);
				Node pn = core_n.get(idx);
				if (pn != null && childrenN.contains(pn)) {
					flag = true;
				}
				if (!flag) { // parentN does not contain corresponding node
					R_succ = false;
					break;
				}
			}
		}
		return true;
	}

	public boolean verifyR_in(Node n, Node m, Vector<Node> in_n, Vector<Node> in_m, Graph NG,
			Graph MG) {
		HashSet<Node> predN = NG.GetParents(n);
		HashSet<Node> succN = NG.GetChildren(n);
		HashSet<Node> predM = MG.GetParents(m);
		HashSet<Node> succM = MG.GetChildren(m);

		predN.retainAll(in_n);
		predM.retainAll(in_m);
		succN.retainAll(in_n);
		succM.retainAll(in_m);

		if ((succN.size() >= succM.size()) && (predN.size() >= predM.size())) {
			return true;
		}
		return false;
	}

	public boolean verifyR_out(Node n, Node m, Vector<Node> out_n, Vector<Node> out_m, Graph NG,
			Graph MG) {
		HashSet<Node> predN = NG.GetParents(n);
		HashSet<Node> succN = NG.GetChildren(n);
		HashSet<Node> predM = MG.GetParents(m);
		HashSet<Node> succM = MG.GetChildren(m);

		predN.retainAll(out_n);
		predM.retainAll(out_m);
		succN.retainAll(out_n);
		succM.retainAll(out_m);

		if ((succN.size() >= succM.size()) && (predN.size() >= predM.size())) {
			return true;
		}
		return false;
	}

	public boolean verifyR_new(Node n, Node m, Vector<Node> core_n, Vector<Node> core_m,
			Vector<Node> in_n, Vector<Node> in_m, Vector<Node> out_n, Vector<Node> out_m, Graph NG,
			Graph MG) {
		HashSet<Node> predN = NG.GetParents(n);
		HashSet<Node> succN = NG.GetChildren(n);
		HashSet<Node> predM = MG.GetParents(m);
		HashSet<Node> succM = MG.GetChildren(m);

		// LinkedList<Node> NVset = NG.GetNodeSet();
		// LinkedList<Node> MVset = MG.GetNodeSet();
		HashSet<Node> NVHSet = new HashSet<Node>();
		HashSet<Node> MVHSet = new HashSet<Node>();

		NVHSet.addAll(NG.GetNodeSet().values());
		NVHSet.removeAll(core_n);
		NVHSet.removeAll(in_n);

		MVHSet.addAll(MG.GetNodeSet().values());
		MVHSet.removeAll(core_m);
		MVHSet.removeAll(in_m);

		predN.retainAll(NVHSet);
		predM.retainAll(MVHSet);
		if (predN.size() < predM.size()) {
			return false;
		}

		succN.retainAll(NVHSet);
		succM.retainAll(MVHSet);
		if (succN.size() < succM.size()) {
			return false;
		}
		return true;
	}

	public boolean testFeasibility(Node[] pair, Vector<Node> core_n, Vector<Node> core_m,
			Vector<Node> in_n, Vector<Node> in_m, Vector<Node> out_n, Vector<Node> out_m) {
		boolean ans = false;
		Node n = pair[0];
		Node m = pair[1];

		// pair[0] : node n in G
		// pair[1] : node m in Q

		/**
		 * Verify R_pred(s, n, m)
		 */
		boolean R_pred = false;
		if (this.verifyR_pred(n, m, core_n, core_m, this.G, this.Q)) {
			R_pred = true;
		} else {
			return false;
		}

		/**
		 * Verify R_succ(s, n, m)
		 */
		boolean R_succ = false;
		if (this.verifyR_succ(n, m, core_n, core_m, this.G, this.Q)) {
			R_succ = true;
		} else {
			return false;
		}

		/**
		 * Verify R_in(s, n, m)
		 */
		boolean R_in = false;
		if (this.verifyR_in(n, m, in_n, in_m, this.G, this.Q)) {
			R_in = true;
		} else {
			return false;
		}

		/**
		 * Verify R_out(s, n, m)
		 */
		boolean R_out = false;
		if (this.verifyR_out(n, m, out_n, out_m, this.G, this.Q)) {
			R_out = true;
		} else {
			return false;
		}

		/**
		 * Verify R_new(s, n, m)
		 */
		boolean R_new = false;
		if (this.verifyR_new(n, m, core_n, core_m, in_n, in_m, out_n, out_m, this.G, this.Q)) {
			R_new = true;
		} else {
			return false;
		}

		if (R_pred && R_succ && R_in && R_out && R_new) {
			ans = true;
		}
		return ans;
	}

	@SuppressWarnings("unchecked")
	public void Match(Vector<Node> core_1, Vector<Node> core_2, Vector<Node> in_1,
			Vector<Node> in_2, Vector<Node> out_1, Vector<Node> out_2) {
		if (core_2.containsAll(this.Q.GetNodeSet().values())) {
			for (int i = 0; i < core_1.size(); i++) {
				System.out.println("Matches: " + core_1.get(i).GetID() + ", "
						+ core_1.get(i).GetAttribute() + " : " + core_2.get(i).GetID() + ", "
						+ core_2.get(i).GetAttribute());
			}
			this.num++;
			System.out.println("This is the " + this.num + " match we find.");
			System.out.println();
		} else {
			// iteratively merges new matches
			Vector<Node[]> canIso = this.findIso(in_1, in_2, out_1, out_2, core_1, core_2);
			for (Node[] pair : canIso) {
				if (this.testFeasibility(pair, core_1, core_2, in_1, in_2, out_1, out_2)) {
					Node n = pair[0];
					Node m = pair[1];

					// create new data structures for depth first search
					Vector<Node> core_1_dup = (Vector<Node>) core_1.clone();
					Vector<Node> core_2_dup = (Vector<Node>) core_2.clone();
					Vector<Node> in_1_dup = (Vector<Node>) in_1.clone();
					Vector<Node> in_2_dup = (Vector<Node>) in_2.clone();
					Vector<Node> out_1_dup = (Vector<Node>) out_1.clone();
					Vector<Node> out_2_dup = (Vector<Node>) out_2.clone();

					// put a pair of new matches in corresponding sets
					core_1_dup.add(pair[0]);
					core_2_dup.add(pair[1]);

					/**
					 * Synchronise other data structures
					 */
					// syn in_1
					HashSet<Node> parentN = this.G.GetParents(n);
					in_1_dup.addAll(parentN);
					in_1_dup.removeAll(core_1_dup);

					// syn out_1
					HashSet<Node> childrenN = this.G.GetChildren(n);
					out_1_dup.addAll(childrenN);
					out_1_dup.removeAll(core_1_dup);

					// syn in_2
					HashSet<Node> parentM = this.Q.GetParents(m);
					in_2_dup.addAll(parentM);
					in_2_dup.removeAll(core_2_dup);
					/** in_2 ²»ÄÜ°üº¬core_2ÖÐµÄµã */

					// syn out_2
					HashSet<Node> childrenM = this.Q.GetChildren(m);
					out_2_dup.addAll(childrenM);
					out_2_dup.removeAll(core_2_dup);
					/** out_2 ²»ÄÜ°üº¬core_2ÖÐµÄµã */
					this.Match(core_1_dup, core_2_dup, in_1_dup, in_2_dup, out_1_dup, out_2_dup);
				}
			}
		}
	}

	public void Match_II(Vector<Node> core_1, Vector<Node> core_2, Vector<Node> in_1,
			Vector<Node> in_2, Vector<Node> out_1, Vector<Node> out_2) {
		if (core_2.containsAll(this.Q.GetNodeSet().values())) {
			this.flag = true;
		} else {
			// iteratively merges new matches
			Vector<Node[]> canIso = this.findIso(in_1, in_2, out_1, out_2, core_1, core_2);
			for (Node[] pair : canIso) {
				if (this.testFeasibility(pair, core_1, core_2, in_1, in_2, out_1, out_2)) {
					Node n = pair[0];
					Node m = pair[1];

					// create new data structures for depth first search
					Vector<Node> core_1_dup = (Vector<Node>) core_1.clone();
					Vector<Node> core_2_dup = (Vector<Node>) core_2.clone();
					Vector<Node> in_1_dup = (Vector<Node>) in_1.clone();
					Vector<Node> in_2_dup = (Vector<Node>) in_2.clone();
					Vector<Node> out_1_dup = (Vector<Node>) out_1.clone();
					Vector<Node> out_2_dup = (Vector<Node>) out_2.clone();

					// put a pair of new matches in corresponding sets
					core_1_dup.add(pair[0]);
					core_2_dup.add(pair[1]);

					/**
					 * Synchronise other data structures
					 */
					// syn in_1
					HashSet<Node> parentN = this.G.GetParents(n);
					in_1_dup.addAll(parentN);
					in_1_dup.removeAll(core_1_dup);

					// syn out_1
					HashSet<Node> childrenN = this.G.GetChildren(n);
					out_1_dup.addAll(childrenN);
					out_1_dup.removeAll(core_1_dup);

					// syn in_2
					HashSet<Node> parentM = this.Q.GetParents(m);
					in_2_dup.addAll(parentM);
					in_2_dup.removeAll(core_2_dup);
					/** in_2 ²»ÄÜ°üº¬core_2ÖÐµÄµã */

					// syn out_2
					HashSet<Node> childrenM = this.Q.GetChildren(m);
					out_2_dup.addAll(childrenM);
					out_2_dup.removeAll(core_2_dup);
					/** out_2 ²»ÄÜ°üº¬core_2ÖÐµÄµã */
					this.Match_II(core_1_dup, core_2_dup, in_1_dup, in_2_dup, out_1_dup, out_2_dup);
				}
			}
		}
	}

	/**
	 * @param u
	 *            : node in Q
	 * @param v
	 *            : node in G
	 */
	public boolean CallMatch_II(Node u, Node v) {
		Vector<Node> core_1 = new Vector<Node>();
		Vector<Node> core_2 = new Vector<Node>();
		Vector<Node> in_1 = new Vector<Node>();
		Vector<Node> in_2 = new Vector<Node>();
		Vector<Node> out_1 = new Vector<Node>();
		Vector<Node> out_2 = new Vector<Node>();
		core_1.add(v);
		core_2.add(u);
		in_1.addAll(this.G.GetParents(v));
		in_1.removeAll(core_1);
		in_2.addAll(this.Q.GetParents(u));
		in_2.removeAll(core_2);
		out_1.addAll(this.G.GetChildren(v));
		out_1.removeAll(core_1);
		out_2.addAll(this.Q.GetChildren(u));
		out_2.removeAll(core_2);
		this.Match_II(core_1, core_2, in_1, in_2, out_1, out_2);
		if (this.flag) {
			return true;
		}
		return false;
	}
}
