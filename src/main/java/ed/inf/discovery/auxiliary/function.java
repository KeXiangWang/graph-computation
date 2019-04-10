package ed.inf.discovery.auxiliary;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import ed.inf.grape.graph.Graph;
import ed.inf.grape.graph.Node;

public class function {
	
	public HashSet<Integer> IsoCheck(Graph Q, int u_index, int[] v_index_set, Graph G) {
		HashMap<Integer, Node> vset = G.GetNodeSet();
		VF2 vf2 = new VF2(G, Q);
		Node u = Q.GetNodeSet().get(u_index);
		HashSet<Integer> ans = new HashSet<Integer>();
		for (int idx : v_index_set) {
			Node v = vset.get(idx);
			boolean ismatch = vf2.CallMatch_II(u, v);
			if (ismatch) {
				ans.add(idx);
			}
			vf2.flag = false;
		}
		return ans;
	}

	public static void main(String[] args) {

		// function f = new function();
		// int[] v_index_set = { 1, 2, 3 };
		// Vector<Integer> set = f.ISOCheck(p, 2, v_index_set, g);
		// for (int i : set) {
		// System.out.println(i);
		// }
		// VF2 vf2 = new VF2(g, p);
		// Vector<Node> core_1 = new Vector<Node>();
		// Vector<Node> core_2 = new Vector<Node>();
		// Vector<Node> in_1 = new Vector<Node>();
		// Vector<Node> in_2 = new Vector<Node>();
		// Vector<Node> out_1 = new Vector<Node>();
		// Vector<Node> out_2 = new Vector<Node>();
		// vf2.Match(core_1, core_2, in_1, in_2, out_1, out_2);

	}

}
