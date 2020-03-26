package com.jiangcoder.lucene.index;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;


public class SearcherUtil {
	private Directory directory;
	private IndexReader reader;
	private String[] ids = {"1","2","3","4","5","6"};//来源，商品描述，商品id
	private String[] titles = {"T-shirt","jacket","jeans","skirt","coat","pants"};//商品名
	private double[]price={176.90,2333.45,100,300.50,122.32,77};//，价格
	private String[] contents = {	"good","very good","beautiful","bad","good",	"shit"};//商品描述
	private int[] source = {1,1,2,1,2,1}; //来源 0 -->all 1-->taobao  2-->jd
	private String[] ranges={"2","7","5","9","0","5"};
	private String[] datetime ={"2009-01-01","2009-01-02","2009-01-08","2009-01-11","2009-01-02","2009-01-15"}; // time
	public SearcherUtil() {
		try {
			directory = FSDirectory.open(new File("/workspace/soft/lucene/index03"));
			index();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	
	public void index() {
		IndexWriter writer = null;
		try {
			writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35)));
			writer.deleteAll();
			Document doc = null;
			for(int i=0;i<ids.length;i++) {
				doc = new Document();
				doc.add(new Field("id",ids[i],Field.Store.YES,Field.Index.NOT_ANALYZED_NO_NORMS));
				doc.add(new Field("content",contents[i],Field.Store.YES,Field.Index.ANALYZED));
				doc.add(new Field("title",titles[i],Field.Store.YES,Field.Index.NOT_ANALYZED_NO_NORMS));
				//存储商品来源
				doc.add(new NumericField("source",Field.Store.YES,true).setIntValue(source[i]));
				//存储日期
				doc.add(new Field("starttime",datetime[i],Field.Store.YES,Field.Index.NOT_ANALYZED_NO_NORMS));
				doc.add(new NumericField("price",Field.Store.YES,true).setDoubleValue(price[i]));
				doc.add(new Field("range",ranges[i],Field.Store.YES,Field.Index.NOT_ANALYZED_NO_NORMS));
				writer.addDocument(doc);
			}
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer!=null)writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public IndexSearcher getSearcher() {
		try {
			if(reader==null) {
				reader = IndexReader.open(directory);
			} else {
				IndexReader tr = IndexReader.openIfChanged(reader);
				if(tr!=null) {
					reader.close();
					reader = tr;
				}
			}
			return new IndexSearcher(reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public IndexSearcher getSearcher(Directory directory) {
		try {
			if(reader==null) {
				reader = IndexReader.open(directory);
			} else {
				IndexReader tr = IndexReader.openIfChanged(reader);
				if(tr!=null) {
					reader.close();
					reader = tr;
				}
			}
			return new IndexSearcher(reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	
	
	/**
	 * jiangtao
	 * @param num
	 */
	public void searchByBoolean(int num,int source) {
		try {
			IndexSearcher searcher = getSearcher();
			BooleanQuery query = new BooleanQuery();
			/*
			 * BooleanQuery可以连接多个子查询
			 * Occur.MUST表示必须出现
			 * Occur.SHOULD表示可以出现
			 * Occur.MUSE_NOT表示不能出现
			 */
			query.add(new TermQuery(new Term("title","coat")), Occur.MUST_NOT);
			query.add(new TermQuery(new Term("content","game")),Occur.SHOULD);
			String field = "starttime";
			TermRangeQuery rangeQuery = new TermRangeQuery(field,"2009-01-01","2009-01-05",true,true);
			Query numericquery = NumericRangeQuery.newDoubleRange("price",100.00, 500.00,true,true);
			query.add(rangeQuery, Occur.MUST);
			query.add(numericquery,Occur.MUST);
			if(source!=0){
				Query sourcecquery = NumericRangeQuery.newIntRange("source",source, source,true,true);
				query.add(sourcecquery,Occur.MUST);
			}
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println("商品id为:"+doc.get("id")+",price:"+doc.get("price")+",title:"+doc.get("title"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void searchByTerm(String field,String name,int num) {
		try {
			IndexSearcher searcher = getSearcher();
			Query query = new TermQuery(new Term(field,name));
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void searchByTermRange(String field,String start,String end,int num) {
		try {
			IndexSearcher searcher = getSearcher();
			Query query = new TermRangeQuery(field,start,end,true, true);
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void searchByNumricRange(String field,int start,int end,int num) {
		try {
			IndexSearcher searcher = getSearcher();
			Query query = NumericRangeQuery.newIntRange(field,start, end,true,true);
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 前缀搜索
	 * @param field
	 * @param value
	 * @param num
	 */
	public void searchByPrefix(String field,String value,int num) {
		try {
			IndexSearcher searcher = getSearcher();
			Query query = new PrefixQuery(new Term(field,value));
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 通配符搜索
	 * @param field
	 * @param value
	 * @param num
	 */
	public void searchByWildcard(String field,String value,int num) {
		try {
			IndexSearcher searcher = getSearcher();
			//在传入的value中可以使用通配符:?和*,?表示匹配一个字符，*表示匹配任意多个字符
			Query query = new WildcardQuery(new Term(field,value));
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 短语搜索
	 * @param num
	 */
	public void searchByPhrase(int num) {
		try {
			IndexSearcher searcher = getSearcher();
			PhraseQuery query = new PhraseQuery();
			query.setSlop(3);
			query.add(new Term("content","very"));
			//第一个Term
			query.add(new Term("content","good"));
			//产生距离之后的第二个Term
//			query.add(new Term("content","football"));
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 模糊查询
	 * @param num
	 */
	public void searchByFuzzy(int num) {
		try {
			IndexSearcher searcher = getSearcher();
			FuzzyQuery query = new FuzzyQuery(new Term("title","oat"),0.4f,0);
			System.out.println(query.getPrefixLength());
			System.out.println(query.getMinSimilarity());
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void searchByQueryParse(Query query,int num) {
		try {
			IndexSearcher searcher = getSearcher();
			TopDocs tds = searcher.search(query, num);
			System.out.println("一共查询了:"+tds.totalHits);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("title")+"["+doc.get("content")+"]-->"+doc.get("id")+","+
						doc.get("price")+","+doc.get("starttime"));
			}
			searcher.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void searchPage(String query,int pageIndex,int pageSize) {
		try {
			Directory dir = FileIndexUtils.getDirectory();
			IndexSearcher searcher = getSearcher(dir);
			QueryParser parser = new QueryParser(Version.LUCENE_35,"content",new StandardAnalyzer(Version.LUCENE_35));
			Query q = parser.parse(query);
			TopDocs tds = searcher.search(q, 500);
			ScoreDoc[] sds = tds.scoreDocs;
			int start = (pageIndex-1)*pageSize;
			int end = pageIndex*pageSize;
			for(int i=start;i<end;i++) {
				Document doc = searcher.doc(sds[i].doc);
				System.out.println(sds[i].doc+":"+doc.get("path")+"-->"+doc.get("filename"));
			}
			
			searcher.close();
		} catch (org.apache.lucene.queryParser.ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据页码和分页大小获取上一次的最后一个ScoreDoc
	 */
	private ScoreDoc getLastScoreDoc(int pageIndex,int pageSize,Query query,IndexSearcher searcher) throws IOException {
		if(pageIndex==1)return null;//如果是第一页就返回空
		int num = pageSize*(pageIndex-1);//获取上一页的数量
		TopDocs tds = searcher.search(query, num);
		return tds.scoreDocs[num-1];
	}
	
	public void searchPageByAfter(String query,int pageIndex,int pageSize) {
		try {
			Directory dir = FileIndexUtils.getDirectory();
			IndexSearcher searcher = getSearcher(dir);
			QueryParser parser = new QueryParser(Version.LUCENE_35,"content",new StandardAnalyzer(Version.LUCENE_35));
			Query q = parser.parse(query);
			//先获取上一页的最后一个元素
			ScoreDoc lastSd = getLastScoreDoc(pageIndex, pageSize, q, searcher);
			//通过最后一个元素搜索下页的pageSize个元素
			TopDocs tds = searcher.searchAfter(lastSd,q, pageSize);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(sd.doc+":"+doc.get("path")+"-->"+doc.get("filename"));
			}
			searcher.close();
		} catch (org.apache.lucene.queryParser.ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void searchNoPage(String query) {
		try {
			Directory dir = FileIndexUtils.getDirectory();
			IndexSearcher searcher = getSearcher(dir);
			QueryParser parser = new QueryParser(Version.LUCENE_35,"content",new StandardAnalyzer(Version.LUCENE_35));
			Query q = parser.parse(query);
			TopDocs tds = searcher.search(q, 20);
			ScoreDoc[] sds = tds.scoreDocs;
			
			for(int i=0;i<sds.length;i++) {
				Document doc = searcher.doc(sds[i].doc);
				System.out.println(sds[i].doc+":"+doc.get("path")+"-->"+doc.get("filename"));
			}
			System.out.println(sds.length);
			searcher.close();
		} catch (org.apache.lucene.queryParser.ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}