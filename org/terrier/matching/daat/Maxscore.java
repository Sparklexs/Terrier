package org.terrier.matching.daat;

import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.terrier.matching.BaseMatching;
import org.terrier.matching.MatchingQueryTerms;
import org.terrier.matching.PostingListManager;
import org.terrier.matching.ResultSet;
import org.terrier.structures.Index;
import org.terrier.structures.postings.IterablePosting;

//import antlr.collections.List;

public class Maxscore extends BaseMatching
{
	PostingListManager	            plm;
	ArrayList<Integer>	            newPostingIndex; // 重排序后的倒排链索引
	double[]	                    accumulation;	 // 最大累加分数
	List<Map.Entry<String, Double>>	maxscores;	     // 每个term对应的最大分数，并按降序排列

	public Maxscore(Index index)
	{
		super(index);
	}

	@Override
	/** 这里要实现按照倒排链的最大分数进行排序 */
	protected void initialisePostings(MatchingQueryTerms queryTerms)
	{
		try
		{
			long start = System.currentTimeMillis();
			plm = new PostingListManager(index,
			        index.getCollectionStatistics(), queryTerms);
			plm.prepare(true);

			PostingListManager plm2 = new PostingListManager(index,
			        super.collectionStatistics, queryTerms);
			plm2.prepare(true);

			TreeMap<String, Double> maxscoresmap = new TreeMap<String, Double>();
			newPostingIndex = new ArrayList<Integer>(plm2.size());

			// 获取每个倒排链最大分数
			int postscount = plm2.size();
			for (int i = 0; i < postscount; i++)
			{
				maxscoresmap.put(plm2.getTerm(i), 0.0);

				// IterablePosting ip = plm.getPosting(i);
				// 倒排链的尽头无法回复,只能另外获取一遍postinglist
				// LexiconEntry t = lexicon
				// .getLexiconEntry(queryTerms.getTerms()[i]);
				// IterablePosting ip = queryTerms.getRequest().getIndex()
				// .getInvertedIndex().getPostings((BitIndexPointer) t);
				IterablePosting ip = plm2.getPosting(i);

				// 部分代码用于输出每个term对应的docid链，已被隐掉
				// File idfile = new File(plm2.getTerm(i));
				// idfile.delete();
				// idfile.createNewFile();
				// FileWriter fWriter = new FileWriter(idfile);
				// int j = 0;

				int currentdocID = ip.getId();
				while (currentdocID != IterablePosting.EOL)
				{
					double tempscore = plm2.score(i);
					if (tempscore > maxscoresmap.get(plm2.getTerm(i)))
					{
						maxscoresmap.put(plm2.getTerm(i), tempscore);
					}

					// fWriter.write(j + " " + currentdocID + "\r\n");
					// j++;

					currentdocID = ip.next();
				}

				// fWriter.flush();
				// fWriter.close();

			}
			maxscores = new ArrayList<Map.Entry<String, Double>>(
			        maxscoresmap.entrySet());
			Collections.sort(maxscores, new mycomp());
			// 对倒排链的顺序进行重排
			for (int i = 0; i < maxscores.size(); i++)
			{
				Map.Entry<String, Double> mapping = maxscores.get(i);

				for (int j = 0; j < plm2.size(); j++)
				{
					if (plm2.getTerm(j) == mapping.getKey())
					{
						newPostingIndex.add(i, j);
					}
				}
			}
			updateAccumulation();
			plm2.close();
			long end = System.currentTimeMillis();

			logger.info("time wasted: " + (end - start) / 1000.0d);

			// 至此，初始化过程已经准备就绪，可以开始匹配打分
		}
		catch (IOException e)
		{
			logger.debug(e);
		}
	}

	@SuppressWarnings("resource")
	@Override
	public ResultSet match(String queryNumber, MatchingQueryTerms queryTerms)
	        throws IOException
	{
		initialise(queryTerms);
		// 为0则全部检索，否则只获取top-k
		if (RETRIEVED_SET_SIZE != 0)
		{
			// Check whether we need to match an empty query. If so, then return
			// the
			// existing result set.

			// long starttime = System.currentTimeMillis();

			String[] queryTermStrings = queryTerms.getTerms();
			if (MATCH_EMPTY_QUERY && queryTermStrings.length == 0)
			{
				resultSet.setExactResultSize(collectionStatistics
				        .getNumberOfDocuments());
				resultSet.setResultSize(collectionStatistics
				        .getNumberOfDocuments());
				return resultSet;
			}

			numberOfRetrievedDocuments = 0;
			boolean targetResultSetSizeReached = false;
			// 该最小查询词同时还是对应多个倒排链的索引数目，它减去1就是倒数第一个倒排链的索引值(wrong)
			int minTermsIncluded = newPostingIndex.size();

			Queue<CandidateResult> candidateResultList = new PriorityQueue<CandidateResult>(
			        RETRIEVED_SET_SIZE);

			double threshold = 0.0; /* getminScore(candidateResultList, topk); */

			// int times = 0;
			Score: while (newPostingIndex.size() > 0 && minTermsIncluded > 0)
			{
				// times++;
				// 获取当前最小查询词集合中的最小文档ID，
				// 这里要在至少包含一个查询词集T中查找DocID，不必在所有倒排链中寻找
				int currentDocIdPostingListIndex = selectMinPostingId(minTermsIncluded);
				IterablePosting currentDocIdPosting = plm
				        .getPosting(newPostingIndex
				                .get(currentDocIdPostingListIndex));
				int currentDocId = currentDocIdPosting.getId();
				CandidateResult currentCandidate = makeCandidateResult(currentDocId);
				// 这里对应的是newPostingIndex的Index顺序，在plm中的顺序还需要get方法获取
				for (int i = currentDocIdPostingListIndex; i < newPostingIndex
				        .size(); i++)
				{
					IterablePosting iPosting = plm.getPosting(newPostingIndex
					        .get(i));
					if (currentCandidate.getScore() + accumulation[i] < threshold)
					{
						break;
					}
					else
					{
						// 当前链中是否含有该doc
						long docIdfound = iPosting.getId();
						if (docIdfound < currentDocId)
						{
							docIdfound = iPosting.next(currentDocId);
						}
						if (docIdfound == currentDocId)
						{
							// 打分
							assignScore(newPostingIndex.get(i),
							        currentCandidate);
						}
						else if (docIdfound == IterablePosting.EOL)
						{
							// 当前链是否到了尽头
							// 下面的if语句好像没有用处
							if (currentDocIdPostingListIndex == i)
							{
								currentDocIdPostingListIndex = -1;
							}
							newPostingIndex.remove(i);
							updateAccumulation();
							minTermsIncluded = updateMinTermsIncluded(
							        minTermsIncluded, threshold);
							if (newPostingIndex.size() == 0)
							{
								break Score;
							}
							// 这里注意i--
							i--;
						}
					}
				}
				// 在此之前要重新判断当前的postingindex和posting，因为之前很有可能该posting到头后已经被剔除

				if ((!targetResultSetSizeReached)
				        || currentCandidate.getScore() > threshold)
				{
					// 分数超过topk阈值或者尚未填满
					candidateResultList.add(currentCandidate);
					// 超过topk个
					if (candidateResultList.size() > RETRIEVED_SET_SIZE) // 大于还是等于
					{
						targetResultSetSizeReached = true;
						candidateResultList.poll();
					}
					if (newPostingIndex.size() == 0)
					{
						break Score;
					}
					threshold = getminScore(candidateResultList,
					        RETRIEVED_SET_SIZE);
					minTermsIncluded = updateMinTermsIncluded(minTermsIncluded,
					        threshold);
				}
				// 更新队列以寻找最小DocID
				for (int i = 0; i < minTermsIncluded; i++)
				{
					IterablePosting ip = plm.getPosting(newPostingIndex.get(i));
					int docid = ip.getId();
					if (docid == currentDocId)
					{
						docid = ip.next();
						if (docid == IterablePosting.EOL)
						{
							newPostingIndex.remove(i);
							if (newPostingIndex.size() == 0)
							{
								break;
							}
							updateAccumulation();
							minTermsIncluded = updateMinTermsIncluded(
							        minTermsIncluded, threshold);
							i--;
						}
					}
				}
			}
			plm.close();

			// Fifth, we build the result set
			resultSet = makeResultSet(candidateResultList);
			numberOfRetrievedDocuments = resultSet.getScores().length;
			finalise(queryTerms);

			// long endtime = System.currentTimeMillis();
			// double timeescaped = (endtime - starttime) / 1000.d;
			// logger.info("用时：" + timeescaped);
		}
		else
		{
//			// The first step is to initialise the arrays of scores and document
//			// ids.
//			plm = new PostingListManager(index, super.collectionStatistics,
//			        queryTerms);
//			plm.prepare(true);

			// Check whether we need to match an empty query. If so, then return
			// the
			// existing result set.
			String[] queryTermStrings = queryTerms.getTerms();
			if (MATCH_EMPTY_QUERY && queryTermStrings.length == 0)
			{
				resultSet.setExactResultSize(collectionStatistics
				        .getNumberOfDocuments());
				resultSet.setResultSize(collectionStatistics
				        .getNumberOfDocuments());
				return resultSet;
			}

			// the number of documents with non-zero score.
			numberOfRetrievedDocuments = 0;

			// The posting list min heap for minimum selection
			// 设置一个定长（queryterm's
			// length）的优先队列，保证每次优先计算文档号最小的文档，当多个查询词对应同一文档时，优先处理前面的查询词文档
			LongPriorityQueue postingHeap = new LongHeapPriorityQueue();

			// The posting list iterator array (one per term) and initialization
			for (int i = 0; i < plm.size(); i++)
			{
				long docid = plm.getPosting(i).getId();
				assert (docid != IterablePosting.EOL);
				postingHeap.enqueue((docid << 32) + i);
			}
			boolean targetResultSetSizeReached = false;
			Queue<CandidateResult> candidateResultList = new PriorityQueue<CandidateResult>();
			int currentDocId = selectMinimumDocId(postingHeap);
			IterablePosting currentPosting = null;
			double threshold = 0.0d;
			// int scored = 0;

			while (currentDocId != -1)
			{
				// We create a new candidate for the doc id considered
				CandidateResult currentCandidate = makeCandidateResult(currentDocId);

				int currentPostingListIndex = (int) (postingHeap.firstLong() & 0xFFFF);
				long nextDocid;
				// System.err.println("currentDocid="+currentDocId+" currentPostingListIndex="+currentPostingListIndex);
				currentPosting = plm.getPosting(currentPostingListIndex);
				// scored++;
				do
				{
					assignScore(currentPostingListIndex, currentCandidate);
					// assignScore(currentPostingListIndex,
					// wm[currentPostingListIndex], currentCandidate,
					// currentPosting);
					long newDocid = currentPosting.next();
					postingHeap.dequeueLong();
					if (newDocid != IterablePosting.EOL)
						postingHeap.enqueue((newDocid << 32)
						        + currentPostingListIndex);
					else if (postingHeap.isEmpty())
						break;
					long elem = postingHeap.firstLong();
					currentPostingListIndex = (int) (elem & 0xFFFF);
					currentPosting = plm.getPosting(currentPostingListIndex);
					nextDocid = elem >>> 32;
				} while (nextDocid == currentDocId);

				if ((!targetResultSetSizeReached)
				        || currentCandidate.getScore() > threshold)
				{
					// System.err.println("New document " +
					// currentCandidate.getDocId() + " with score " +
					// currentCandidate.getScore() + " passes threshold of " +
					// threshold);
					candidateResultList.add(currentCandidate);
					if (RETRIEVED_SET_SIZE != 0
					        && candidateResultList.size() == RETRIEVED_SET_SIZE + 1)
					{
						targetResultSetSizeReached = true;
						candidateResultList.poll();
						// System.err.println("Removing document with score " +
						// candidateResultList.poll().getScore());
					}
					// System.err.println("Now have " +
					// candidateResultList.size() +
					// " retrieved docs");
					threshold = candidateResultList.peek().getScore();
				}
				currentDocId = selectMinimumDocId(postingHeap);
			}

			// System.err.println("Scored " + scored + " documents");
			plm.close();

			// Fifth, we build the result set
			resultSet = makeResultSet(candidateResultList);
			numberOfRetrievedDocuments = resultSet.getScores().length;
			finalise(queryTerms);
		}
		return resultSet;
	}

	protected CandidateResultSet makeResultSet(
	        Queue<CandidateResult> candidateResultList)
	{
		return new CandidateResultSet(candidateResultList);
	}

	protected CandidateResult makeCandidateResult(int currentDocId)
	{
		return new CandidateResult(currentDocId);
	}

	/**
	 * assign the score for this posting to this candidate result.
	 * 
	 * @param i
	 *            which query term index this represents
	 * @param cc
	 *            the candidate result object for this document
	 * @throws IOException
	 */
	protected void assignScore(final int i, CandidateResult cc)
	        throws IOException
	{
		cc.updateScore(plm.score(i));
		cc.updateOccurrence((i < 16) ? (short) (1 << i) : 0);
	}

	/** returns the docid of the lowest posting */
	protected final int selectMinimumDocId(final LongPriorityQueue postingHeap)
	{
		return (postingHeap.isEmpty()) ? -1
		        : (int) (postingHeap.firstLong() >>> 32);
	}

	protected final int selectMinPostingId(int minTermsIncluded)
	{
		int docid = Integer.MAX_VALUE, postingid = 0;
		for (int i = 0; i < minTermsIncluded; i++)
		{
			int temp = plm.getPosting(newPostingIndex.get(i)).getId();
			if (temp != IterablePosting.EOL && temp < docid)
			{
				docid = temp;
				postingid = i;
			}
		}
		return postingid;
	}

	double getminScore(Queue<CandidateResult> list, int topk)
	{
		double min = 0;
		// int si = list.size();
		if (!list.isEmpty() && list.size() == topk)
		{
			min = list.peek().getScore();
		}
		return min;
	}

	int updateMinTermsIncluded(int min, double threshold)
	{
		int newmin = min;
		for (int i = accumulation.length - 1; i >= 0; i--)
		{
			if (accumulation[i] > threshold)
			{
				newmin = i + 1;
				break;
			}
		}
		return newmin;
	}

	void updateAccumulation()
	{
		accumulation = new double[newPostingIndex.size() + 1];
		accumulation[newPostingIndex.size()] = 0;
		// 计算重排序后的每个倒排链的最大累计分数
		for (int i = newPostingIndex.size() - 1; i >= 0; i--)
		{
			Map.Entry<String, Double> mapping = maxscores.get(i);
			accumulation[i] = accumulation[i + 1] + mapping.getValue();
		}
	}

	/** {@inheritDoc} */
	@Override
	public String getInfo()
	{
		return "daat.Maxscore";
	}

	class mycomp implements Comparator<Map.Entry<String, Double>>
	{
		public int compare(Map.Entry<String, Double> o1,
		        Map.Entry<String, Double> o2)
		{
			return o2.getValue().compareTo(o1.getValue());
		}
	}

}
