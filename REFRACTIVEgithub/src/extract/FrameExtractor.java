/**
 * Refractive is a tool for extracting knowledge from syntactic and semantic relations.
 * Copyright © 2013 Peter Exner
 * 
 * This file is part of Refractive.
 *
 * Refractive is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Refractive is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Refractive.  If not, see <http://www.gnu.org/licenses/>.
 */

package extract;

import id.UniqueIdGenerator;
import io.Frame;
import io.Slot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.mapreduce.Reducer.Context;

import conll.model.Document;
import conll.model.Edge;
import conll.model.Edge.EdgeType;
import conll.model.Sentence;
import conll.model.Span;
import conll.model.Span.SpanType;
import conll.model.Token;
import conll.model.TokenPropertyIdentifier;

public class FrameExtractor {
	private static UniqueIdGenerator uniqueIdGenerator;
	
	public static UniqueIdGenerator getUniqueIdGenerator() {
		return uniqueIdGenerator;
	}
	
	public static void setUniqueIdGenerator(UniqueIdGenerator uniqueIdGenerator) {
		FrameExtractor.uniqueIdGenerator = uniqueIdGenerator;
	}
	
	public static List<Frame> extractFrames(Document conllDocument, int frameHeight, @SuppressWarnings("rawtypes") Context context) throws Exception {
		List<Frame> frames = new ArrayList<Frame>();

		for(Sentence sentence:conllDocument.getSentences()) {
			TreeMap<Long, TreeSet<Token>> frameTokenMap = extractFrameTokens(sentence, frameHeight);

			for(Entry<Long, TreeSet<Token>> entry:frameTokenMap.entrySet()) {
				frames.add(convertFrameTokensToFrame(entry.getValue(), entry.getKey()));
			}
			
			List<Span> neSpans = sentence.getSpansBySpanType(SpanType.NE);
			context.getCounter("wikipedia", "named entity count").increment(neSpans.size());
			
			Collection<Token> predicateTokens = sentence.getPredicateTokens();
			context.getCounter("wikipedia", "predicates").increment(predicateTokens.size());
			
			for(Span neSpan:neSpans) {
				boolean foundNeSpan = false;
				for(Entry<Long, TreeSet<Token>> entry:frameTokenMap.entrySet()) {
					for(Token frameToken:entry.getValue()) {
						if(tokenIsHeadTokenInSpan(frameToken, neSpan)) {
							context.getCounter("frames", "named entity count").increment(1);
							foundNeSpan = true;
							break;
						}
					}
					
					if(foundNeSpan) {
						break;
					}
				}
			}
			
			for(Token predicateToken:predicateTokens) {
				boolean foundPredicateToken = false;
				for(Entry<Long, TreeSet<Token>> entry:frameTokenMap.entrySet()) {
					for(Token frameToken:entry.getValue()) {
						if(predicateToken == frameToken) {
							context.getCounter("frames", "predicate token count").increment(1);
							foundPredicateToken = true;
							break;
						}
					}
					
					if(foundPredicateToken) {
						break;
					}
				}
			}
			
		}

		return frames;
	}
	
	public static List<Frame> extractFramesNoStats(Document conllDocument, int frameHeight) throws Exception {
		List<Frame> frames = new ArrayList<Frame>();

		for(Sentence sentence:conllDocument.getSentences()) {
			TreeMap<Long, TreeSet<Token>> frameTokenMap = extractFrameTokens(sentence, frameHeight);

			for(Entry<Long, TreeSet<Token>> entry:frameTokenMap.entrySet()) {
				frames.add(convertFrameTokensToFrame(entry.getValue(), entry.getKey()));
			}
		}

		return frames;
	}

	private static TreeMap<Long, TreeSet<Token>> extractFrameTokens(Sentence sentence, int frameHeight) throws Exception {
		TreeMap<Long, TreeSet<Token>> frameTokenMap = new TreeMap<Long, TreeSet<Token>>();
		List<Edge> edgeList = sentence.getRootToken().getEdgesByType(EdgeType.PDEPENDENCY);

		if((edgeList.size() == 1) && tokenIsNounOrVerb(edgeList.get(0).getFrom())) {
			Long frameId = uniqueIdGenerator.nextLong();
			frameTokenMap.put(frameId, new TreeSet<Token>());
			extractFrameTokensIter(edgeList.get(0).getFrom(), frameHeight, frameHeight, frameId, frameTokenMap);
		}

		return frameTokenMap;
	}

	private static void extractFrameTokensIter(Token token, int frameHeight, int level, long frameId, TreeMap<Long, TreeSet<Token>> frameTokenMap) throws Exception {
		List<Edge> edgeList = token.getEdgesByType(EdgeType.PDEPENDENCY);
		
		if(level == -1) {
			if(tokenIsNounOrVerb(token)) {
				Long newFrameId = uniqueIdGenerator.nextLong();
				
				frameTokenMap.put(newFrameId, new TreeSet<Token>());
				token.setProperty(TokenPropertyIdentifier.FEAT, frameId + ":" + newFrameId);
				extractFrameTokensIter(token, frameHeight, frameHeight, newFrameId, frameTokenMap);
			} else {
				for(Edge edge:edgeList) {
					Token fromToken = edge.getFrom();
					extractFrameTokensIter(fromToken, frameHeight, -1, frameId, frameTokenMap);
				}
			}
		} else if(level == 0) {
			frameTokenMap.get(frameId).add(token);
			
			if((edgeList.size() > 0)) {
				if(tokenIsNounOrVerb(token)) {
					Long newFrameId = uniqueIdGenerator.nextLong();
					
					frameTokenMap.put(newFrameId, new TreeSet<Token>());
					token.setProperty(TokenPropertyIdentifier.FEAT, frameId + ":" + newFrameId);
					extractFrameTokensIter(token, frameHeight, frameHeight, newFrameId, frameTokenMap);
				} else {
					for(Edge edge:edgeList) {
						Token fromToken = edge.getFrom();
						extractFrameTokensIter(fromToken, frameHeight, -1, frameId, frameTokenMap);
					}
				}
			}
		} else {
			frameTokenMap.get(frameId).add(token);
			
			for(Edge edge:edgeList) {
				Token fromToken = edge.getFrom();
				extractFrameTokensIter(fromToken, frameHeight, level - 1, frameId, frameTokenMap);
			}
		}
	}

	private static Frame convertFrameTokensToFrame(TreeSet<Token> frameTokens, long frameId) throws Exception {
		Frame frame = new Frame();
		frame.setFrameId(frameId);

		String relation;
		String headCoreferenceMention;
		String yield;
		String namedEntityType;
		Boolean isProperNoun;

		for(Token frameToken:frameTokens) {
			String ppos = frameToken.getProperty(TokenPropertyIdentifier.PPOS);
			
			if(ppos.equalsIgnoreCase("NNP") || ppos.equalsIgnoreCase("NNPS")) {
				isProperNoun = true;
			} else {
				isProperNoun = false;
			}
			
			if(tokenIsFrameRootToken(frameToken, frameTokens)) {
				if(ppos.startsWith("NN")) {
					relation = "NOUN";
				} else if(ppos.startsWith("VB")) {
					relation = "VERB";
				} else {
					relation = frameToken.getProperty(TokenPropertyIdentifier.PDEPREL);
				}
			} else {
				relation = frameToken.getProperty(TokenPropertyIdentifier.PDEPREL);
			}

			Slot slot = new Slot();
			slot.setRelation(relation);
			slot.setValue(getLemmaOrFrameReference(frameToken, frameId));
			slot.setIsProperNoun(isProperNoun);
			frame.getSlots().add(slot);
			
			
			headCoreferenceMention = getHeadCoreferenceMention(frameToken);
			if(!headCoreferenceMention.equals("")) {
				slot = new Slot();
				slot.setRelation(relation + "-C");
				slot.setValue(headCoreferenceMention);
				slot.setIsProperNoun(isProperNoun);
				frame.getSlots().add(slot);
			}

			yield = getYield(frameToken, frameTokens, new ArrayList<String>(Arrays.asList("DT", "IN")));
			if(!yield.equals("")) {
				slot = new Slot();
				slot.setRelation(relation + "-Y");
				slot.setValue(yield);
				slot.setIsProperNoun(isProperNoun);
				frame.getSlots().add(slot);
			}

			namedEntityType = getNamedEntity(frameToken);
			if(!namedEntityType.equals("")) {
				slot = new Slot();
				slot.setRelation(relation + "-T");
				slot.setValue(namedEntityType);
				slot.setIsProperNoun(isProperNoun);
				frame.getSlots().add(slot);
			}
			
			emitSemanticSlots(frameToken, frameTokens, frame, isProperNoun);
		}

		return frame;
	}

	private static void emitSemanticSlots(Token frameToken, TreeSet<Token> frameTokens, Frame frame, boolean isProperNoun) throws Exception {
		String predicate = frameToken.getProperty(TokenPropertyIdentifier.PRED);
		Slot slot;
		String relation;
		String yield;
		
		if(!predicate.equalsIgnoreCase("_")) {
			slot = new Slot();
			relation = predicate.toUpperCase();
			slot.setRelation(relation);
			slot.setValue(frameToken.getProperty(TokenPropertyIdentifier.PLEMMA));
			slot.setIsProperNoun(isProperNoun);
			frame.getSlots().add(slot);
			
			yield = getYield(frameToken, frameTokens, new ArrayList<String>(Arrays.asList("DT", "IN")));
			if(!yield.equals("")) {
				slot = new Slot();
				slot.setRelation(relation + "-Y");
				slot.setValue(yield);
				slot.setIsProperNoun(isProperNoun);
				frame.getSlots().add(slot);
			}
		}
		
		List<Edge> edgeList = frameToken.getSentence().getEdgesByFromType(frameToken, EdgeType.SEMANTICROLE);
		
		for(Edge edge:edgeList) {
			predicate = edge.getTo().getProperty(TokenPropertyIdentifier.PRED);
			if(!predicate.equalsIgnoreCase("_")) {
				slot = new Slot();
				relation = predicate.toUpperCase() + "_" + edge.getLabel();
				slot.setRelation(relation);
				slot.setValue(frameToken.getProperty(TokenPropertyIdentifier.PLEMMA));
				slot.setIsProperNoun(isProperNoun);
				frame.getSlots().add(slot);
				
				yield = getYield(frameToken, frameTokens, new ArrayList<String>(Arrays.asList("DT", "IN")));
				if(!yield.equals("")) {
					slot = new Slot();
					slot.setRelation(relation + "-Y");
					slot.setValue(yield);
					slot.setIsProperNoun(isProperNoun);
					frame.getSlots().add(slot);
				}
			}
		}
	}

	private static String getLemmaOrFrameReference(Token frameToken, long frameId) throws Exception {
		String[] frameReference = frameToken.getProperty(TokenPropertyIdentifier.FEAT).split(":");
		if(frameReference.length == 2) {
			if(Long.valueOf(frameReference[0]).longValue() == frameId) {
				return ("Frame " + frameReference[1]);
			} else {
				return (frameToken.getProperty(TokenPropertyIdentifier.PLEMMA));
			}
		} else {
			return (frameToken.getProperty(TokenPropertyIdentifier.PLEMMA));
		}
	}

	private static String getYield(Token frameToken, TreeSet<Token> frameTokens, List<String> excludePOS) throws Exception {
		TreeSet<Token> yield = new  TreeSet<Token>();
		getYieldRecursive(frameToken, frameTokens, yield);

		StringBuilder sb = new StringBuilder();
		for(Token yieldToken:yield) {
			if(!excludePOS.contains(yieldToken.getProperty(TokenPropertyIdentifier.PPOS))) {
				sb.append(yieldToken.getProperty(TokenPropertyIdentifier.FORM) + " ");
			}
		}

		return sb.toString().trim();
	}

	private static void getYieldRecursive(Token frameToken, TreeSet<Token> frameTokens, TreeSet<Token> yield) {
		if(frameTokens.contains(frameToken)) {
			yield.add(frameToken);

			List<Edge> edgeList = frameToken.getEdgesByType(EdgeType.PDEPENDENCY);
			for(Edge edge:edgeList) {
				getYieldRecursive(edge.getFrom(), frameTokens, yield);
			}
		}
	}

	private static String getNamedEntity(Token frameToken) throws Exception {
		List<Span> namedEntitySpans = frameToken.getSentence().getSpansBySpanType(SpanType.NE);

		String namedEntityLabel = "";
		for(Span namedEntitySpan:namedEntitySpans) {
			if(tokenIsHeadTokenInSpan(frameToken, namedEntitySpan)) {
				namedEntityLabel = namedEntitySpan.getLabel();
			}
		}

		return namedEntityLabel;
	}

	private static String getHeadCoreferenceMention(Token frameToken) throws Exception {
		List<Span> corefSpans = frameToken.getSentence().getSpansBySpanType(SpanType.COREF);

		String corefLabel = "";
		for(Span corefSpan:corefSpans) {
			if(tokenIsHeadTokenInSpan(frameToken, corefSpan)) {
				corefLabel = corefSpan.getLabel();
				break;
			}
		}

		StringBuilder corefSpan = new StringBuilder();

		if(!corefLabel.equalsIgnoreCase("")) {
			if(!corefLabel.endsWith("*")) {
				corefLabel = corefLabel + "*";
			}

			List<Span> corefDocumentSpans = frameToken.getSentence().getDocument().getSpansBySpanTypeAndLabel(SpanType.COREF, corefLabel);

			if(corefDocumentSpans.size() == 1) {
				for(Token token:corefDocumentSpans.get(0).getTokenSpan()) {
					corefSpan.append(token.getProperty(TokenPropertyIdentifier.FORM) + " ");
				}		
			}
		}

		return corefSpan.toString().trim();
	}

	private static boolean tokenIsHeadTokenInSpan(Token token, Span span) throws Exception {
		int startTokenId = Integer.valueOf(span.getStartToken().getProperty(TokenPropertyIdentifier.ID));
		int endTokenId = Integer.valueOf(span.getEndToken().getProperty(TokenPropertyIdentifier.ID));
		
		
		for(Token headToken:span.getTokenSpan()) {
			int headTokenParentId = Integer.parseInt(headToken.getProperty(TokenPropertyIdentifier.PHEAD));
			
			if(headTokenParentId < startTokenId || headTokenParentId > endTokenId) {
				return token == headToken;
			}
		}
		
		return false;
	}

	private static boolean tokenIsNounOrVerb(Token token) throws Exception {
		String ppos = token.getProperty(TokenPropertyIdentifier.PPOS);

		if(ppos.startsWith("NN") || ppos.startsWith("VB")) {
			return true;
		} else {
			return false;
		}		
	}

	private static boolean tokenIsFrameRootToken(Token token, TreeSet<Token> frameTokens) {
		List<Edge> edgeList = token.getSentence().getEdgesByFromType(token, EdgeType.PDEPENDENCY);

		for(Edge edge:edgeList) {
			if(frameTokens.contains(edge.getTo())) {
				return false;
			}
		}

		return true;
	}
}