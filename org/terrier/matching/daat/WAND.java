/**
 * 
 */
package org.terrier.matching.daat;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntIntHashMap;
import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.httpclient.methods.PostMethod;
import org.mockito.exceptions.verification.NeverWantedButInvoked;
import org.terrier.matching.BaseMatching;
import org.terrier.matching.MatchingQueryTerms;
import org.terrier.matching.PostingListManager;
import org.terrier.matching.ResultSet;
import org.terrier.structures.Index;
import org.terrier.structures.postings.IterablePosting;

/**
 * @author John
 *
 */
public class WAND extends BaseMatching
{

	class PostingWithIndex
	{
		IterablePosting	ip;
		int		        index;

		public PostingWithIndex(IterablePosting _ip, int _index)
		{
			ip = _ip;
			index = _index;
		}
	}

	/**
	 * 
	 */
	PostingListManager	              plm;
	double[]	                      accumulation;	              // 最大累加分数
	double[]	                      accumulation2;
	TreeMap<PostingWithIndex, Double>	postsManager;
	List<Map.Entry<Integer, Integer>>	postsMan;
	TIntDoubleHashMap	              maxscores;
	CompForDocid	                  mycomp	= new CompForDocid();

	class CompForDocid implements Comparator<Map.Entry<Integer, Integer>>
	{
		@Override
		public int compare(Entry<Integer, Integer> o1,
		        Entry<Integer, Integer> o2)
		{
			return o1.getValue() - o2.getValue();
		}
	}

	public WAND(Index _index)
	{
		super(_index);
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

			postsManager = new TreeMap<PostingWithIndex, Double>(
			        new Comparator<PostingWithIndex>() {
				        @Override
				        public int compare(PostingWithIndex o1,
				                PostingWithIndex o2)
				        {
					        return o1.ip.getId() == o2.ip.getId() ? (o1.index - o2.index)
					                : (o1.ip.getId() - o2.ip.getId());
				        }
			        });
			TreeMap<Integer, Integer> tmpMap = new TreeMap<>();
			maxscores = new TIntDoubleHashMap();

			// 获取每个倒排链最大分数
			int postscount = plm2.size();
			for (int i = 0; i < postscount; i++)
			{
				// maxscoresmap.put(plm2.getTerm(i), 0.0);
				double maxscore = -1;
				IterablePosting ip = plm2.getPosting(i);
				double tempscore = 5000000;
				int currentdocID = ip.getId();

				while (currentdocID != IterablePosting.EOL)
				{
					tempscore = plm2.score(i);
					if (tempscore > maxscore)
						maxscore = tempscore;
					currentdocID = ip.next();
				}
				if (plm.getPosting(i).getId() != IterablePosting.EOL)
				{
					postsManager.put(
					        new PostingWithIndex(plm.getPosting(i), i),
					        maxscore);
					tmpMap.put(i, plm.getPosting(i).getId());
					maxscores.put(i, maxscore);
				}
			}
			postsMan = new ArrayList<>(tmpMap.entrySet());
			Collections.sort(postsMan, mycomp);

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

	private void updateAccumulation()
	{
		if (postsManager.size() > 0)
		{
			accumulation = new double[postsManager.size()];
			accumulation[0] = 0;
			for (int i = 0; i < postsManager.size(); i++)
				accumulation[i] += (i == 0) ? (postsManager.firstEntry()
				        .getValue()) : (accumulation[i - 1] + postsManager
				        .values().toArray(new Double[0])[i].doubleValue());
			accumulation2 = new double[postsMan.size()];
			accumulation2[0] = 0;
			for (int i = 0; i < postsMan.size(); i++)
			{
				accumulation2[i] += (i == 0) ? maxscores.get(postsMan.get(i)
				        .getKey()) : (accumulation[i - 1] + maxscores
				        .get(postsMan.get(i).getKey()));

			}
		}
	}

	/** returns the docid of the lowest posting */
	protected final int selectMinimumDocId(final LongPriorityQueue postingHeap)
	{
		return (postingHeap.isEmpty()) ? -1
		        : (int) (postingHeap.firstLong() >>> 32);
	}

	protected CandidateResult makeCandidateResult(int currentDocId)
	{
		return new CandidateResult(currentDocId);
	}

	protected CandidateResultSet makeResultSet(
	        Queue<CandidateResult> candidateResultList)
	{
		return new CandidateResultSet(candidateResultList);
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

	@Override
	public String getInfo()
	{
		return "daat.WAND";
	}

	@SuppressWarnings("resource")
	@Override
	public ResultSet match(String queryNumber, MatchingQueryTerms queryTerms)
	        throws IOException
	{
		initialise(queryTerms);
		if (RETRIEVED_SET_SIZE != 0)
		{
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

			Queue<CandidateResult> candidateResultList = new PriorityQueue<CandidateResult>();
			int currentDocId = 0;
			int currentDocId2 = 0;
			CandidateResult currentCandidate = null;
			PostingWithIndex currentPosting = null;
			double threshold = 0.0d;
			wloop: while (postsManager.size() > 0)
			{
				// 寻找pivot所在的posting和指向的docid
				int pivotIndex = -1;
				for (int i = 0; i < accumulation.length; i++)
					if (accumulation[i] >= threshold)
					{
						// currentPostingIndex = i;
						currentPosting = postsManager.keySet().toArray(
						        new PostingWithIndex[0])[i];
						currentDocId = currentPosting.ip.getId();
						currentDocId2 = postsMan.get(i).getValue();
						pivotIndex = i;
						currentCandidate = makeCandidateResult(currentDocId);
						break;
					}
				// 如果所有的maxscore之和也不能满足threshold，直接退出
				// XXX 由于acuum不停地删除postings，有可能会遇到以下情况
				if (accumulation[accumulation.length - 1] < threshold)
				{
					break wloop;
				}
				// 判断在此之前的posting指向的docid是否与现在一致，不包含本posting
				Set<Map.Entry<PostingWithIndex, Double>> sets = postsManager
				        .headMap(currentPosting).entrySet();
				int ssize = sets.size();
				if (pivotIndex > 0)
				{
					for (int i = 0; i < pivotIndex; i++)
					{
						if (currentDocId2 == postsMan.get(i).getValue())
							continue;
						int tmpDocid = plm.getPosting(postsMan.get(i).getKey())
						        .next(currentDocId2);
						if (tmpDocid == IterablePosting.EOL)
						{
							postsMan.remove(i);
							if (postsMan.size() == 0)
							{
								break wloop;
							}
							// 这里不需要重排序，之前的多次循环最多只是加到与pivot相等，因而队列次序并没有发生变化
							updateAccumulation();
							continue wloop;
						}
						else if (tmpDocid > currentDocId2)
						{
							postsMan.get(i).setValue(tmpDocid);
							Collections.sort(postsMan, mycomp);
							updateAccumulation();
							continue wloop;
						}
						postsMan.get(i).setValue(tmpDocid);
					}
				}
				if (ssize > 0)
				{
					Iterator<Map.Entry<PostingWithIndex, Double>> postsBeforeCurrent = sets
					        .iterator();
					while (postsBeforeCurrent.hasNext())
					{
						Map.Entry<PostingWithIndex, Double> ipEntry = postsBeforeCurrent
						        .next();
						int tmpDocid = ipEntry.getKey().ip.getId();
						if (tmpDocid == currentDocId)
							continue;
						tmpDocid = ipEntry.getKey().ip.next(currentDocId);
						// 如果当前posting到达尾部，删除之并更新accumulation
						if (tmpDocid == IterablePosting.EOL)
						{
							postsBeforeCurrent.remove();
							if (postsManager.size() == 0)
							{
								break wloop;
							}
							updateAccumulation();
							continue wloop;
						}
						// 如果之前posting并不包含currentDocid且并未到达队尾，重新选择pivot
						// 这里删除又加入是为了实现重排序而设置的，目前没有什么好办法
						else if (tmpDocid > currentDocId)
						{
							// ipentry在remove后发生变化，皆因它只是引用
							PostingWithIndex pwi = new PostingWithIndex(
							        ipEntry.getKey().ip, ipEntry.getKey().index);
							Double max = ipEntry.getValue();
							postsBeforeCurrent.remove();
							postsManager.put(pwi, max);
							updateAccumulation();
							continue wloop;
						}
						// 默认情况，就是等于pivot，此时无需重排序，到打分后移动指针再重排
					}

					// 此时，所有的pivot之前的所有posts均指向同一个docid
					// // 首先更新accum
					// updateAccumulation();
				}
				// 计算分数

				for (int j = 0; j < postsMan.size(); j++)
				{
					if (postsMan.get(j).getValue() == currentDocId2)
					{
						assignScore(postsMan.get(j).getKey(), currentCandidate);
						int tmpDocid = plm.getPosting(postsMan.get(j).getKey())
						        .next();
						if (tmpDocid == IterablePosting.EOL)
							postsMan.remove(j);
						else
							postsMan.get(j).setValue(tmpDocid);
					}
				}
				if (postsMan.size() == 0)
				{
					break wloop;
				}
				Collections.sort(postsMan, mycomp);
				updateAccumulation();

				Iterator<PostingWithIndex> postIterator = postsManager.keySet()
				        .iterator();
				while (postIterator.hasNext())
				{
					PostingWithIndex pwi = postIterator.next();
					if (pwi.ip.getId() == currentDocId)
					{
						assignScore(pwi.index, currentCandidate);
						if (pwi.ip.next() == IterablePosting.EOL)
						{
							postIterator.remove();
						}
					}
				}

				// 全部打分完毕，打分过的posting全部移动指针，现判断是否空
				if (postsManager.size() == 0)
				{
					break wloop;
				}
				// 重排序，没有找到其他可行的办法
				PostingWithIndex[] pwi = postsManager.keySet().toArray(
				        new PostingWithIndex[0]);
				Double[] maxscores = postsManager.values().toArray(
				        new Double[0]);
				postsManager.clear();
				for (int i = 0; i < maxscores.length; i++)
				{
					postsManager.put(pwi[i], maxscores[i]);
				}
				updateAccumulation();

				if (currentCandidate.getScore() > threshold)
				{
					candidateResultList.add(currentCandidate);
					if (candidateResultList.size() == RETRIEVED_SET_SIZE + 1)
					{
						candidateResultList.poll();
						threshold = candidateResultList.peek().getScore();
					}
				}
			}

			plm.close();

			// Fifth, we build the result set
			resultSet = makeResultSet(candidateResultList);
			numberOfRetrievedDocuments = resultSet.getScores().length;
			finalise(queryTerms);
		}
		else
		{
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
			// length）的优先队列，保证每次优先计算文档号最小的文档
			// 当多个查询词对应同一文档时，优先处理前面的查询词文档
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
}
