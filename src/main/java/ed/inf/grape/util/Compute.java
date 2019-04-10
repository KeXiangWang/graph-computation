package ed.inf.grape.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.Pattern;
import ed.inf.discovery.auxiliary.PatternPair;

public class Compute {

	static Logger log = LogManager.getLogger(Compute.class);

	static double maxConfidence = 1.0;

	/** from node label to extract edge type */
	public static int getEdgeType(int nodeLabel) {
//		return nodeLabel / 1000; // TODO 19.3.31 13:26 commented wkx
		return nodeLabel;
	}

	public static void computeConfidence(Pattern p, double coff) {

		if (p.getXNotYCandidates().toArray().length == 0) {
			p.setConfidence(maxConfidence);
			return;
		}

		double confidence = coff;

		confidence = p.getXCandidates().toArray().length * coff
				/ p.getXNotYCandidates().toArray().length;

		p.setConfidence(confidence);

		if (confidence > 1.0) {
			maxConfidence = confidence;
		}
	}

	public static void computeUBConfidence(Pattern p, double coff) {

		if (p.getXNotYCandidates().toArray().length == 0) {
			p.setConfidenceUB(maxConfidence);
			return;
		}
		double confidenceub = 0.0;

//		log.debug("support:" + p.getSupportUB());

		confidenceub = p.getSupportUB() * coff / p.getXNotYCandidates().toArray().length;

		p.setConfidenceUB(confidenceub);
	}

	public static double computeDiff(Pattern p1, Pattern p2) {
		int inter = RoaringBitmap.and(p1.getXCandidates(), p2.getXCandidates()).toArray().length;
		int union = RoaringBitmap.or(p1.getXCandidates(), p2.getXCandidates()).toArray().length;
		return 1 - (inter * 1.0 / union);
	}

	public static double computeDashF(Pattern r1, Pattern r2) {
		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA) * (r1.getConfidence() + r2.getConfidence());
		ret += (2 * KV.PARAMETER_LAMBDA) * computeDiff(r1, r2);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}

	public static double computeBF(Queue<PatternPair> pairList) {

		assert (pairList.size() <= KV.PARAMETER_K);

		List<Pattern> listk = new ArrayList<Pattern>();
		for (PatternPair pr : pairList) {
			listk.add(pr.getP1());
			listk.add(pr.getP2());
		}

		if (listk.size() <= 1) {
			return 1.0;
		}

		double conf = 0.0;
		double dive = 0.0;
		for (int i = 0; i < listk.size(); i++) {
			conf += listk.get(i).getConfidence();
			for (int j = i + 1; j < listk.size(); j++) {
				dive += computeDiff(listk.get(i), listk.get(j));
			}
		}
		double bf = (1 - KV.PARAMETER_LAMBDA) * conf
				+ (2 * KV.PARAMETER_LAMBDA / (listk.size() - 1)) * dive;
		return bf;
	}

	public static double computeLemma1(Pattern p, double maxUconfDeltaE) {

		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA) * (p.getConfidence() + maxUconfDeltaE);
		ret += (2 * KV.PARAMETER_LAMBDA);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}

	public static double computeLemma2(Pattern p, double maxUconfSigma) {

		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA) * (p.getConfidenceUB() + maxUconfSigma);
		ret += (2 * KV.PARAMETER_LAMBDA);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}
}
