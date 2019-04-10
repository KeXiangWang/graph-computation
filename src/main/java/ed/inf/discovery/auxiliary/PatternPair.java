package ed.inf.discovery.auxiliary;

import ed.inf.discovery.Pattern;

public class PatternPair implements Comparable<PatternPair> {

	private Pattern p1;
	private Pattern p2;
	private double f;

	public PatternPair(Pattern p1, Pattern p2, double f) {
		this.p1 = p1;
		this.p2 = p2;
		this.f = f;
	}

	public Pattern getP1() {
		return p1;
	}

	public Pattern getP2() {
		return p2;
	}

	public double getF() {
		return this.f;
	}

	@Override
	public int compareTo(PatternPair o) {
		return this.f - o.f < 0 ? -1 : 1;
	}

	@Override
	public String toString() {
		return "PatternPair [p1=" + p1 + ", p2=" + p2 + ", f=" + f + "]";
	}
}