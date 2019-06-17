package ed.inf.discovery.auxiliary;

import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultEdge;

public class MyEdge extends DefaultEdge {
    private Object myAttribute;
//    DefaultDirectedWeightedGraph
//
//    public MyEdge(Object source, Object target, Object attribute ){
//        LabeledEdge
//    }
    public void setMyAttribute(Object attribute) {
        this.myAttribute = attribute;
    }

    public Object getMyAttribute() {
        return this.myAttribute;
    }

    public String toString() {
        return "(" + this.getSource() + " : " + this.getTarget() + " by " + this.myAttribute + ")";
    }
}
