package ed.inf.discovery;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;

import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.Dev;
import ed.inf.grape.util.IO;
import ed.inf.grape.util.KV;

public class DiscoveryTask {

    private int partitionID;

    /**
     * step count
     */
    private int superstep;

    /**
     * messages generated by this step
     */
    private List<Pattern> generatedMessages;

    static Logger log = LogManager.getLogger(DiscoveryTask.class);

    public DiscoveryTask(int partitionID) {
        this.partitionID = partitionID;
        this.generatedMessages = new LinkedList<Pattern>();
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void startStep(Partition partition) {

        long start = System.currentTimeMillis();
        Pattern initPattern = new Pattern(this.partitionID);

        initPattern.initialXYEdge(KV.QUERY_X_LABEL, KV.QUERY_Y_LABEL, KV.EDGE_X_Y_ATTRIBUTE);

        partition.initWithPattern(initPattern);

        log.debug("init count using " + (System.currentTimeMillis() - start) + "ms.");

        log.debug(partition.getCountInfo());
        log.debug(Dev.currentRuntimeState());
        List<Pattern> expandedPatterns = this.expand(partition, initPattern);
        log.debug("expanded " + expandedPatterns.size() + " patterns.");
        // TODO: automorphism check of the expendedPatterns.
        partition.initCoff(initPattern);
        Comparator<Pattern> cmp = (a, b) -> {
            if (a.getXNotYCandidates().toArray().length - b.getXNotYCandidates().toArray().length < 0) {
                return -1;
            } else if (a.getXNotYCandidates().toArray().length - b.getXNotYCandidates().toArray().length == 0) {
                return 0;
            } else {
                return 1;
            }
        };
        PriorityQueue<Pattern> topNPatterns = new PriorityQueue<>(cmp);
        topNPatterns.add(initPattern);
        start = System.currentTimeMillis();
        for (Pattern p : expandedPatterns) {
            log.debug("pID = " + p.getPatternID() + " origin = " + p.getOriginID()
                    + ", before MatchR X.size() =  " + p.getXCandidates().toArray().length);
            partition.preMatchR(p, topNPatterns, this.superstep );
//			log.debug("pID = " + p.getPatternID() + ", p.xcan = "
//					+ p.getXCandidates().toArray().length + ", xnotycan = "
//					+ p.getXNotYCandidates().toArray().length);
            long suppStart = System.currentTimeMillis();
//            int supportForNextHop = 0;
//            for (int x : p.getXCandidates()) {
//                if (partition.isExtendibleAtR(x, this.superstep + 1)) {
//                    supportForNextHop++;
//                }
//            }
//            p.setSupportUB(supportForNextHop);
            log.debug("support upbound = " + p.getSupportUB() + ", using "
                    + (System.currentTimeMillis() - suppStart) + "ms.");
//			log.debug("generatedMessages.size "+generatedMessages.size());
        }
        generatedMessages.addAll(topNPatterns);
//        log.info("superstep = 1     private List<Pattern> generatedMessages:\n" + generatedMessages);
        log.debug("compute confidence using " + (System.currentTimeMillis() - start) + "ms.");
        log.debug(Dev.currentRuntimeState());
    }

    public void continuesStep(Partition partition, List<Pattern> messages) {
        log.debug("hello continue. reveived message size = " + messages.size());
//        assert false;
        int i = 0;
        Comparator<Pattern> cmp = (a, b) -> {
            if (a.getConfidence() - b.getConfidence() < 0) {
                return -1;
            } else if (a.getConfidence() - b.getConfidence() == 0) {
                return 0;
            } else {
                return 1;
            }
        };
        PriorityQueue<Pattern> topNPatterns = new PriorityQueue<>(cmp);
        topNPatterns.addAll(messages);
        for (Pattern baseMessage : messages) {
            i++;
            log.debug("Currently in step " + this.superstep + " expanded " + i + "/"
                    + messages.size());
            baseMessage.resetAsLocalPattern(partition);
            List<Pattern> expandedPatterns = this.expand(partition, baseMessage);
            log.debug("Expanded " + expandedPatterns.size() + " patterns totally.");
            long start = System.currentTimeMillis();
            for (Pattern p : expandedPatterns) {
//                log.debug("pID = " + p.getPatternID() + " origin = " + p.getOriginID()
//                        + ", beforeXCan =  " + p.getXCandidates().toArray().length + ",XnotYCan= "
//                        + p.getXNotYCandidates().toArray().length);
                partition.preMatchR(p, topNPatterns, this.superstep );
                // int supportForNextHop = 0;
                // for (int x : p.getXCandidates()) {
                // if (partition.isExtendibleAtR(x, this.superstep + 1)) {
                // supportForNextHop++;
                // }
                // }
                //
                // long suppStart = System.currentTimeMillis();
                // p.setSupportUB(supportForNextHop);
                // log.debug("support upbound = " + p.getSupportUB() +
                // ", using "
                // + (System.currentTimeMillis() - suppStart) + "ms.");
                // TODO: to check whether this partition is further expandable.
//                generatedMessages.add(p);
            }
            log.debug("expandtime " + (System.currentTimeMillis() - start) + "ms.");
//			log.debug(Dev.currentRuntimeState());
        }
        generatedMessages.addAll(topNPatterns);
//        log.info(generatedMessages);
        log.debug("current step " + this.superstep + " finished.");
//        if(superstep == 5){
//            log.info("result:" + generatedMessages);
//            log.info("result number:" + generatedMessages.size());
//            assert false;
//        }
    }

    private List<Pattern> expand(Partition partition, Pattern origin) {
        // nodes with attribute are denote hop = -1
        List<Pattern> expandedPattern = new LinkedList<>();
        List<Pattern> expandedWithPersonNode = new LinkedList<>();
        int radiu = this.superstep;
        log.info("This is " + radiu + "th superstep");
//        log.info("origin pattern:" + origin);
        if (radiu == KV.PARAMETER_B) {
            log.info("radiu reaches limit, expend stopped.");
            return expandedPattern;
        }
        expandedWithPersonNode.add(origin);
        // Preparing Finished
        for (Pattern p : expandedWithPersonNode) {
            for (SimpleNode n : p.getQ().vertexSet()) {
                if (n.attribute != KV.QUERY_Y_LABEL) {
                    Pattern np = new Pattern(this.partitionID, p, true);
                    np.expendWildFromFixedNode(n.nodeID);
                    expandedPattern.add(np);
                }
            }
        }

        log.debug("Expand Wild Node finished：");
//        for (Pattern pattern : expandedPattern) {
//            log.info(pattern);
//        }
//        for (Pattern p : expandedWithPersonNode) {
//            log.info("expandedWithPersonNode ----------------------------\n" + p.toString() + " \nin set " + p.getQ().vertexSet());
//            Map<Integer, Integer> attrs = new HashMap<Integer, Integer>();
//
//            for (DefaultWeightedEdge edge : p.getQ().edgeSet()) {
//                attrs.put((int) p.getQ().getEdgeWeight(edge), p.getQ().getEdgeSource(edge).nodeID);
//            }
//            log.info("attrs: " + attrs);
//
//            for (SimpleNode n : p.getQ().vertexSet()) {
//                if (n.hop == radiu && (int)n.attribute != KV.QUERY_Y_LABEL) {
//                    List<Pattern> newGensPatterns = new LinkedList<Pattern>();
//
//                    for (int attr : partition.getFreqEdgeLabels()) {
//                        if (attrs.keySet().contains(attr)) {
//                        } else {
//                            log.info("not match attr(FreqEdgeLabel) " + attr);
//                            Pattern np = new Pattern(this.partitionID, p, true);
////                            log.info("after not match attr" + attr + " ???? "+ np.getQ().vertexSet());
//                            np.expendAttrFromFixedNodeWithAttr(n.nodeID, attr);
//                            newGensPatterns.add(np);
//                        }
//                    }
//
//                    Iterator<Pattern> iterator = newGensPatterns.iterator();
//                    while (iterator.hasNext()) {
//                        Pattern pInGen = iterator.next();
//                        for (Pattern pInRet : expandedPattern) {
//                            if (Pattern.testSamePattern(pInRet, pInGen)) {
//                                iterator.remove();
//                            }
//                        }
//                    }
//                    expandedPattern.addAll(newGensPatterns);
//                }
//            }
//        }
        // TODO to make the algorithm more general, expand() needs to have capacity of add an edge between two existed vertices.
        return expandedPattern;
    }

    public void setSuperstep(long superstep) {
        this.superstep = (int) superstep;
    }

    public void prepareForNextCompute() {
        // TODO: reset messages
        this.generatedMessages.clear();
    }

    public List<Pattern> getMessages() {
        return this.generatedMessages;
    }

    public static void main(String[] args) {

        Pattern p = new Pattern(0);
        p.initialXYEdge(1, 2430004, 2400000);
        p.expendChildFromFixedNodeWithAttr(0, 1);
        p.expendChildFromFixedNodeWithAttr(0, 1);
        // p.expend1Node1EdgeAsChildFromFixedNode(0, 2430010);

        System.out.println(p.toString());

        KV.PARAMETER_B = 4;
        Partition partition = IO.loadPartitionFromVEFile(0, "dataset/graph-0");
        // Partition partition = IO.loadPartitionFromVEFile(0, "dataset/test");
        partition.initWithPattern(p);
        System.out.println(partition.getCountInfo());

        DiscoveryTask task = new DiscoveryTask(0);
        task.superstep = 1;
        List<Pattern> ps = task.expand(partition, p);

        System.out.println("generate size:" + ps.size());

        for (Pattern pattern : ps) {
            // System.out.println(pattern);
            for (SimpleNode _v : pattern.getQ().vertexSet()) {
                StringBuffer _s = new StringBuffer();
                _s.append(_v.nodeID).append("\t").append(_v.attribute);
                for (DefaultWeightedEdge _e : pattern.getQ().outgoingEdgesOf(_v)) {
                    _s.append("\t").append(pattern.getQ().getEdgeTarget(_e).nodeID);
                }
                System.out.println(_s);
                // writer.println(_s);
            }
            System.out.println("----------------------");
        }

    }
}
