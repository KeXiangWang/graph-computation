package ed.inf.discovery;

import java.io.Serializable;

import ed.inf.grape.graph.Edge;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.experimental.equivalence.EquivalenceComparator;
import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;
import org.jgrapht.experimental.isomorphism.GraphIsomorphismInspector;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DefaultEdge;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.discovery.auxiliary.MyEdge;
import ed.inf.grape.graph.Graph;
import ed.inf.grape.graph.Node;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.KV;

public class Pattern implements Serializable {

    private static final long serialVersionUID = 8335552619728619513L;
    @SuppressWarnings("rawtypes")
    private static EquivalenceComparator _vComparator = new VertexComparator();
    @SuppressWarnings("rawtypes")
    private static EquivalenceComparator _eComparator = new EdgeComparator();

    /**
     * pattern ID
     */
    private int patternID;

    /**
     * origin, to get Xs
     */
    private int originID;

    /**
     * origin partition ID
     */
    private int partitionID = 0;

    private DefaultDirectedWeightedGraph<SimpleNode, DefaultWeightedEdge> Q;
    private SimpleNode x;
    private SimpleNode y;

    private RoaringBitmap XCandidates;
    private RoaringBitmap XNotYCandidates;

    public int newlyMatchXCount; // DONE for record the new match count
    public int newlyMatchXNotYCount;

    private double confidence;
    private double confidenceUB;
    private int supportUB = 0;
    private int YCount = 0;
    private int notYCount = 0;
    // private RoaringBitmap discoveredPartitions;

    /**
     * for pattern growing, assign this to new node
     */
    private int currentNodeID = 0;

    /**
     * for pattern growing, assign this to new pattern
     */
    private static int currentGloblePatternID = 0;

    static Logger log = LogManager.getLogger(Pattern.class);

    static {
        _vComparator = new VertexComparator();
        _eComparator = new EdgeComparator();
    }

    public Pattern(int partitionID) {
        this.partitionID = partitionID;
        this.patternID = Pattern.currentGloblePatternID++;
        this.originID = this.patternID;
        this.Q = new DefaultDirectedWeightedGraph<SimpleNode, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        this.currentNodeID = 0;

        this.newlyMatchXCount = 0; // DONE initial the record of the new match count
        this.newlyMatchXNotYCount = 0;

        confidence = 0.0;
        confidenceUB = 0.0;
        // this.discoveredPartitions = new RoaringBitmap();
    }

    public Pattern(int partitionID, Pattern o, boolean changeOrigin) {
        this.partitionID = partitionID;
        this.patternID = Pattern.currentGloblePatternID++;
        if (changeOrigin) {
            this.originID = o.getPatternID();
        } else {
            this.originID = o.originID;
        }
        this.Q = new DefaultDirectedWeightedGraph<SimpleNode, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        Graphs.addGraph(this.Q, o.Q);
        this.x = o.x;
        this.y = o.y;
        this.currentNodeID = o.currentNodeID;


        this.newlyMatchXCount = o.newlyMatchXCount; // DONE initial the record of the new match count
        this.newlyMatchXNotYCount = o.newlyMatchXNotYCount;


        this.confidence = o.confidence;
        this.confidenceUB = o.confidenceUB;
        this.supportUB = o.supportUB;
        this.notYCount = o.notYCount;
        this.YCount = o.YCount;
        this.XCandidates = SerializationUtils.clone(o.XCandidates);
        this.XNotYCandidates = SerializationUtils.clone(o.XNotYCandidates);
    }

    public RoaringBitmap getXCandidates() {
        return XCandidates;
    }

    public void setXCandidates(RoaringBitmap xCandidates) {
        this.XCandidates = xCandidates;
    }

    public RoaringBitmap getXNotYCandidates() {
        return XNotYCandidates;
    }

    public void setXnotYCandidates(RoaringBitmap xCandidates) {
        this.XNotYCandidates = xCandidates;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public double getConfidenceUB() {
        return confidenceUB;
    }

    public void setConfidenceUB(double confidenceUB) {
        this.confidenceUB = confidenceUB;
    }

    public int getSupportUB() {
        return supportUB;
    }

    public void setSupportUB(int supportUB) {
        this.supportUB = supportUB;
    }

    public int getYCount() {
        return YCount;
    }

    public void setYCount(int yCount) {
        YCount = yCount;
    }

    public int getNotYCount() {
        return notYCount;
    }

    public void setNotYCount(int notYCount) {
        this.notYCount = notYCount;
    }

    public void initialXYEdge(int xAttr, int yAttr, int edgeAttribute) {

        SimpleNode nodex = new SimpleNode(this.nextNodeID(), xAttr, 0);
        SimpleNode nodey = new SimpleNode(this.nextNodeID(), yAttr, 1);

        this.Q.addVertex(nodex);
        this.Q.addVertex(nodey);

        DefaultWeightedEdge DefaultWeightedEdge = this.Q.addEdge(nodex, nodey);
        this.Q.setEdgeWeight(DefaultWeightedEdge, edgeAttribute);

        this.x = nodex;
        this.y = nodey;
    }

    public void resetAsLocalPattern(Partition partition) {
        this.XCandidates.and(partition.getX());
        this.XNotYCandidates.and(partition.getXNotY());
        this.YCount = partition.getYCount();
        this.notYCount = partition.getNotYCount();
        this.confidence = 0.0;
        this.confidenceUB = 0.0;
    }

    // public void expend1Node1EdgeAsChildFromFixedNode(int fromNodeID, int
    // toAttr) {
    //
    // for (SimpleNode fromNode : this.Q.vertexSet()) {
    // if (fromNode.nodeID == fromNodeID) {
    // SimpleNode toNode = new SimpleNode(this.nextNodeID(), toAttr,
    // fromNode.hop + 1);
    // this.Q.addVertex(toNode);
    // this.Q.addEdge(fromNode, toNode);
    // return;
    // }
    // }
    // }

    public void expendChildFromFixedNodeWithAttr(int fromNodeID, int toAttr) {

        for (SimpleNode fromNode : this.Q.vertexSet()) {
            if (fromNode.nodeID == fromNodeID) {
                SimpleNode toNode = new SimpleNode(this.nextNodeID(), toAttr, fromNode.hop + 1);
                this.Q.addVertex(toNode);
                DefaultWeightedEdge DefaultWeightedEdge = this.Q.addEdge(fromNode, toNode);
                this.Q.setEdgeWeight(DefaultWeightedEdge, toAttr);
                return;
            }
        }
    }

    public void expendLoopFromFixedNodeWithAttr(int fromNodeID, int toAttr) {

        for (SimpleNode fromNode : this.Q.vertexSet()) {
            if (fromNode.nodeID == fromNodeID) {
                SimpleNode toNode = new SimpleNode(this.nextNodeID(), toAttr, fromNode.hop + 1);
                this.Q.addVertex(toNode);
                DefaultWeightedEdge DefaultWeightedEdge_0 = this.Q.addEdge(fromNode, toNode);
                DefaultWeightedEdge DefaultWeightedEdge_1 = this.Q.addEdge(toNode, fromNode);
                this.Q.setEdgeWeight(DefaultWeightedEdge_0, toAttr);
                this.Q.setEdgeWeight(DefaultWeightedEdge_1, toAttr);
                return;
            }
        }
    }

    public void expendParentFromFixedNodeWithAttr(int toNodeID, int fromAttr) {

        for (SimpleNode toNode : this.Q.vertexSet()) {
            if (toNode.nodeID == toNodeID) {
                SimpleNode fromNode = new SimpleNode(this.nextNodeID(), fromAttr, toNode.hop + 1);
                this.Q.addVertex(fromNode);
                this.Q.addEdge(fromNode, toNode);
                return;
            }
        }
    }

    public void expendAttrFromFixedNodeWithAttr(int fromNodeID, int toAttr) {

        for (SimpleNode fromNode : this.Q.vertexSet()) {
            if (fromNode.nodeID == fromNodeID) {
                SimpleNode attrNode = new SimpleNode(this.nextNodeID(), toAttr, fromNode.hop + 1);
                this.Q.addVertex(attrNode);
                DefaultWeightedEdge DefaultWeightedEdge = this.Q.addEdge(fromNode, attrNode);
                this.Q.setEdgeWeight(DefaultWeightedEdge, toAttr);
                return;
            }
        }
    }

    public boolean expendEdgeFromNodeToNode(int fromNodeID, int toNodeID) {

        SimpleNode fnode = null, tnode = null;

        for (SimpleNode node : this.Q.vertexSet()) {
            if (node.nodeID == fromNodeID) {
                fnode = node;
            }
            if (node.nodeID == toNodeID) {
                tnode = node;
            }
        }

        if (fnode != null && tnode != null) {
            // FIXME: hacking for the MatchR.
            DefaultWeightedEdge DefaultWeightedEdge = this.Q.addEdge(fnode, tnode);

//            this.Q.setEdgeWeight(DefaultWeightedEdge, 21);
            log.info("fnode, tnode" + fnode.hop + "  " + tnode.hop);
//			tnode.hop = fnode.hop + 1;
            return true;
        } else
            return false;
    }

    public void setCoordinatorPatternID(int id) {
        this.patternID = id;
    }

    public int getPartitionID() {
        return this.partitionID;
    }

    public int getPatternID() {
        return this.patternID;
    }

    public int getOriginID() {
        return this.originID;
    }

    public SimpleNode getX() {
        return this.x;
    }

    public SimpleNode getY() {
        return this.y;
    }

    public boolean isValid() {

        log.debug("pattern" + this.patternID + "-diameter = " + this.getDiameter());

        /** diameter gt bound. */
        if (this.getDiameter() > KV.PARAMETER_B) {
            return false;
        }
        // TODO: other test.

        return true;
    }

    public DefaultDirectedWeightedGraph<SimpleNode, DefaultWeightedEdge> getQ() {
        return this.Q;
    }

    private int nextNodeID() {
        return this.currentNodeID++;
    }

    private int getDiameter() {

        int max = 0;
        for (SimpleNode n : this.getQ().vertexSet()) {
            if (n.hop > max) {
                max = n.hop;
            }
        }
        return max;
    }

    private boolean isExtendibleInRound(int r) {
        // TODO: check is extendible in round r
        return true;
    }

    public static boolean testSamePattern(Pattern p1, Pattern p2) {

        if (p1.getQ().edgeSet().size() != p2.getQ().edgeSet().size()
                || p1.getQ().vertexSet().size() != p2.getQ().vertexSet().size()) {
            return false;
        }

        // if (Simulation.compute_match(p.getQ(), ep.getQ())) {
        try {
            @SuppressWarnings("unchecked")
            GraphIsomorphismInspector<DefaultWeightedEdge> gii = AdaptiveIsomorphismInspectorFactory
                    .createIsomorphismInspector(p1.getQ(), p2.getQ(), _vComparator, _eComparator);
            return gii.isIsomorphic();
        } catch (Exception e) {
            return false;
        }

    }

    public static void add(Pattern destination, Pattern addToDest) {

        // FIXME: change add method
        // destination.confidence += addToDest.confidence;
        destination.XCandidates.or(addToDest.XCandidates);
        destination.XNotYCandidates.or(addToDest.XNotYCandidates);
        destination.notYCount += addToDest.notYCount;
        destination.YCount += addToDest.YCount;
    }

    @Override
    public String toString() {
        return "Pattern [patternID=" + patternID + ", originID=" + originID + ", partitionID="
                + partitionID + ", \nQ=" + Q + ", \nx=" + x + ", \ny=" + y + ", \ndiameter="
                + getDiameter() + "]";
    }

    public Graph toPGraph() {

        Graph g = new Graph();
        for (SimpleNode v : this.Q.vertexSet()) {
            Node node = new Node(v.nodeID, v.attribute);
            g.InsNode(node);
        }

        for (DefaultWeightedEdge e : this.Q.edgeSet()) {
            Node sourceNode = g.FindNode(this.Q.getEdgeSource(e).nodeID);
            Node targetNode = g.FindNode(this.Q.getEdgeTarget(e).nodeID);
            Integer attribute = (int) this.Q.getEdgeWeight(e);
            g.InsEdge(sourceNode, targetNode, attribute);
        }
        return g;
    }

    public Graph toQGraph() {

        Graph g = new Graph();
        for (SimpleNode v : this.Q.vertexSet()) {
            if (v != this.x && v != this.y) {
                Node node = new Node(v.nodeID, v.attribute);
                g.InsNode(node);
            }
        }

        for (DefaultWeightedEdge e : this.Q.edgeSet()) {

            int sourceID = this.Q.getEdgeSource(e).nodeID;
            int targetID = this.Q.getEdgeTarget(e).nodeID;
            if (sourceID == x.nodeID && targetID == y.nodeID) {
                continue;
            }
            Integer attribute = (int) this.Q.getEdgeWeight(e);
            Node sourceNode = g.FindNode(sourceID);
            Node targetNode = g.FindNode(targetID);
            g.InsEdge(sourceNode, targetNode, attribute);
        }
        return g;
    }

    // private int getDiameter() {
    // int max = 0;
    // HashMap<SimpleNode, Integer> visited = new HashMap<SimpleNode,
    // Integer>();
    // Queue<SimpleNode> q = new LinkedList<SimpleNode>();
    //
    // for (SimpleNode vf : this.Q.vertexSet()) {
    // visited.clear();
    // q.clear();
    // q.add(vf);
    // visited.put(vf, 0);
    // while (!q.isEmpty()) {
    // SimpleNode v = q.poll();
    // int dist = visited.get(v);
    // for (DefaultWeightedEdge e : this.Q.outgoingEdgesOf(v)) {
    // SimpleNode tv = this.Q.getEdgeTarget(e);
    // if (!visited.keySet().contains(tv)) {
    // q.add(tv);
    // visited.put(tv, dist + 1);
    // }
    // }
    // for (DefaultWeightedEdge e : this.Q.incomingEdgesOf(v)) {
    // SimpleNode fv = this.Q.getEdgeSource(e);
    // if (!visited.keySet().contains(fv)) {
    // q.add(fv);
    // visited.put(fv, dist + 1);
    // }
    // }
    // }
    //
    // for (SimpleNode v : visited.keySet()) {
    // int dist = visited.get(v);
    // if (dist > max) {
    // max = dist;
    // }
    // }
    // }
    // return max;
    // }
    static class VertexComparator implements
            EquivalenceComparator<SimpleNode, org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge>> {

        @Override
        public boolean equivalenceCompare(SimpleNode arg1, SimpleNode arg2,
                                          org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge> context1,
                                          org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge> context2) {
            // TODO Auto-generated method stub

            if (arg1.attribute == arg2.attribute) {
                return true;
            }
            return false;
        }

        @Override
        public int equivalenceHashcode(SimpleNode arg1,
                                       org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge> context) {
            // TODO Auto-generated method stub
            return 0;
        }

    }

    static class EdgeComparator implements
            EquivalenceComparator<DefaultWeightedEdge, org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge>> {

        @Override
        public boolean equivalenceCompare(DefaultWeightedEdge arg1, DefaultWeightedEdge arg2,
                                          org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge> context1,
                                          org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge> context2) {
            // TODO Auto-generated method stub
            return true;
        }

        @Override
        public int equivalenceHashcode(DefaultWeightedEdge arg1,
                                       org.jgrapht.Graph<SimpleNode, DefaultWeightedEdge> context) {
            // TODO Auto-generated method stub
            return 0;
        }

    }

//	public static void main(String[] args) {
//
//		// automorphism test
//
//		Pattern p1 = new Pattern(0);
//		p1.initialXYEdge(1, 41);
//		// p1.expend1Node1EdgeAsChildFromFixedNode(0, 10);
//
//		Pattern p2 = new Pattern(0);
//		p2.initialXYEdge(1, 10);
//		// p2.expend1Node1EdgeAsChildFromFixedNode(0, 41);
//
//		System.out.println(p1.toString());
//		System.out.println(p2.toString());
//
//		System.out.println(Pattern.testSamePattern(p1, p2));
//	}
}
