package com.jary.spark_hadoop.mllib;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class IkParticiple implements Participle, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -294144242634420143L;

	public String[] participle(String content, boolean b) {
		IKAnalyzer analyzer = new IKAnalyzer(b);
		try {
			return analysisResult(analyzer, content);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			analyzer.close();
		}
		return null;
	}

	private String[] analysisResult(Analyzer analyzer, String keyWord) {

		StringReader reader = new StringReader(keyWord);
		List<String> wordList = new ArrayList<String>();
		try {
			TokenStream tokenStream = analyzer.tokenStream("", reader);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				CharTermAttribute termAttribute = tokenStream
						.getAttribute(CharTermAttribute.class);
				wordList.add(termAttribute.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return wordList.toArray(new String[wordList.size()]);
	}
	
	public static void main(String[] args) {
		Participle participle = new IkParticiple();
		String[] result = participle.participle("本报讯（记者 马海邻）记者昨天得到确切消息，新浪公司今天将宣布由曹国伟接替汪延担任CEO，汪延则任董事会重要职位。 曹国伟此前是新浪总裁兼CFO（首席财务官）。他于1999年加入新浪，任主管财务的副总，之后任职CFO、CFO兼COO（首席运营官）。曹国伟先后获得复旦大学新闻学学士、美国奥克拉荷马大学新闻学硕士学位，1993年获德国奥斯町大学商业管理学院财务专业硕士学位后，任职普华永道。其人被外界评价为强势的鹰派管理风格，曾获《首席财务官》杂志和IDG中国共同评选的“2005年度杰出CFO”。"
				+ "外界认为，由曹国伟主导的2003年1月收购讯龙、2004年3月收购深圳网兴科技，这两笔收购不仅奠定了新浪此后在无线增值业务上的地位，同时也增加了新浪经营模式的多样性与稳定性，对新浪意义非凡。新浪董事会对公司2005年以来业绩的滑坡、非广告业务收入下滑感到忧虑，希望曹国伟能够力挽狂澜，加强新业务的拓展。"
				+ "汪延于2003年5月取代茅道临出任新浪CEO。有关新浪再次换帅的传闻，起于去年下半年。昨晚新浪内部一名高层人士证实，今天在发布财务季报的同时，宣布人事变动.", true);
		System.out.println(Arrays.toString(result));
	}
}