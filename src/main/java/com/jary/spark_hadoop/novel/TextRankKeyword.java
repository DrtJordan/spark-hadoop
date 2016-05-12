package com.jary.spark_hadoop.novel;
//package cn.hadoop.spark_hadoop.novel;
//
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Queue;
//import java.util.Set;
//import java.util.TreeMap;
//import java.util.TreeSet;
//
//import com.hankcs.hanlp.algoritm.MaxHeap;
//import com.hankcs.hanlp.seg.Segment;
//import com.hankcs.hanlp.seg.common.Term;
//import com.hankcs.hanlp.summary.KeywordExtractor;
//import com.hankcs.hanlp.tokenizer.NotionalTokenizer;
//
///**
// * @author hjl
// * @date 2016年9月7日 上午11:30:34 
// */
//public class TextRankKeyword extends KeywordExtractor {
//	
//	private int nKeyword;
//	static final float d = 0.85F;
//    static final int max_iter = 200;
//    static final float min_diff = 0.001F;
//    static final boolean $assertionsDisabled = !com.hankcs.hanlp.summary.TextRankKeyword.class.desiredAssertionStatus();
//    private static NotionalTokenizer defaultSegment;
//    
//    static {
//    	defaultSegment = new NotionalTokenizer();
//    }
//
//
//    public List getKeyword(String content)
//    {
//        Set entrySet = getTermAndRank(content, Integer.valueOf(nKeyword)).entrySet();
//        List result = new ArrayList(entrySet.size());
//        java.util.Map.Entry entry;
//        for(Iterator iterator = entrySet.iterator(); iterator.hasNext(); result.add(entry.getKey()))
//            entry = (java.util.Map.Entry)iterator.next();
//
//        return result;
//    }
//
//    public Map getTermAndRank(String content)
//    {
//		if(!$assertionsDisabled && content == null)
//        {
//            throw new AssertionError();
//        } else
//        {
//            List termList = defaultSegment.segment(content);
//            return getRank(termList);
//        }
//    }
//
//    public Map getTermAndRank(String content, Integer size) {
//        Map map = getTermAndRank(content);
//        Map result = new LinkedHashMap();
//        java.util.Map.Entry entry;
//        for(Iterator iterator = (new MaxHeap(size.intValue(), new Comparator() {
//
//        	public int compare(java.util.Map.Entry o1, java.util.Map.Entry o2) {
//        		return ((Float)o1.getValue()).compareTo((Float)o2.getValue());
//        	}
//
//        	public int compare(Object obj, Object obj1) {
//        		return compare((java.util.Map.Entry)obj, (java.util.Map.Entry)obj1);
//        	}
//        })).addAll(map.entrySet()).toList().iterator(); iterator.hasNext(); result.put(entry.getKey(), entry.getValue()) )
//        	entry = (java.util.Map.Entry)iterator.next();
//        		
//        return result;
//	}
//
//    public Map getRank(List<Term> termList)
//    {
//        List wordList = new ArrayList(termList.size());
//        Iterator<Term> words = termList.iterator();
//        do
//        {
//            if(!words.hasNext())
//                break;
//            Term t = (Term)words.next();
//            if(shouldInclude(t))
//                wordList.add(t.word);
//        } while(true);
//        words = new TreeMap();
//        Queue que = new LinkedList();
//        for(Iterator iterator = wordList.iterator(); iterator.hasNext();)
//        {
//            String w = (String)iterator.next();
//            if(!words.containsKey(w))
//                words.put(w, new TreeSet());
//            que.offer(w);
//            if(que.size() > 5)
//                que.poll();
//            Iterator iterator1 = que.iterator();
//            while(iterator1.hasNext()) 
//            {
//                String w1 = (String)iterator1.next();
//                Iterator iterator2 = que.iterator();
//                while(iterator2.hasNext()) 
//                {
//                    String w2 = (String)iterator2.next();
//                    if(!w1.equals(w2))
//                    {
//                        ((Set)words.get(w1)).add(w2);
//                        ((Set)words.get(w2)).add(w1);
//                    }
//                }
//            }
//        }
//
//        Map score = new HashMap();
//        int i = 0;
//        do
//        {
//            if(i >= 200)
//                break;
//            Map m = new HashMap();
//            float max_diff = 0.0F;
//            String key;
//label0:
//            for(Iterator iterator3 = words.entrySet().iterator(); iterator3.hasNext(); max_diff = Math.max(max_diff, Math.abs(((Float)m.get(key)).floatValue() - (score.get(key) != null ? ((Float)score.get(key)).floatValue() : 0.0F))))
//            {
//                java.util.Map.Entry entry = (java.util.Map.Entry)iterator3.next();
//                key = (String)entry.getKey();
//                Set value = (Set)entry.getValue();
//                m.put(key, Float.valueOf(0.15F));
//                Iterator iterator4 = value.iterator();
//                do
//                {
//                    if(!iterator4.hasNext())
//                        continue label0;
//                    String element = (String)iterator4.next();
//                    int size = ((Set)words.get(element)).size();
//                    if(!key.equals(element) && size != 0)
//                        m.put(key, Float.valueOf(((Float)m.get(key)).floatValue() + (0.85F / (float)size) * (score.get(element) != null ? ((Float)score.get(element)).floatValue() : 0.0F)));
//                } while(true);
//            }
//
//            score = m;
//            if(max_diff <= 0.001F)
//                break;
//            i++;
//        } while(true);
//        return score;
//    }
//    
//    
//	/**
//	 * @return the nKeyword
//	 */
//	public int getnKeyword() {
//		return nKeyword;
//	}
//	/**
//	 * @param nKeyword the nKeyword to set
//	 */
//	public void setnKeyword(int nKeyword) {
//		this.nKeyword = nKeyword;
//	}
//	
//}
