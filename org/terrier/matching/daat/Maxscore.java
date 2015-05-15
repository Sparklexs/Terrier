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
	ArrayList<Integer>	            newPostingIndex; // �������ĵ���������
	double[]	                    accumulation;	 // ����ۼӷ���
	List<Map.Entry<String, Double>>	maxscores;	     // ÿ��term��Ӧ����������������������

	public Maxscore(Index index)
	{
		super(index);
	}

	@Override
	/** ����Ҫʵ�ְ��յ��������������������� */
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

			// ��ȡÿ��������������
			int postscount = plm2.size();
			for (int i = 0; i < postscount; i++)
			{
				maxscoresmap.put(plm2.getTerm(i), 0.0);

				// IterablePosting ip = plm.getPosting(i);
				// �������ľ�ͷ�޷��ظ�,ֻ�������ȡһ��postinglist
				// LexiconEntry t = lexicon
				// .getLexiconEntry(queryTerms.getTerms()[i]);
				// IterablePosting ip = queryTerms.getRequest().getIndex()
				// .getInvertedIndex().getPostings((BitIndexPointer) t);
				IterablePosting ip = plm2.getPosting(i);

				// ���ִ����������ÿ��term��Ӧ��docid�����ѱ�����
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
			// �Ե�������˳���������
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

			// ���ˣ���ʼ�������Ѿ�׼�����������Կ�ʼƥ����
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
		// Ϊ0��ȫ������������ֻ��ȡtop-k
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
			// ����С��ѯ��ͬʱ���Ƕ�Ӧ�����������������Ŀ������ȥ1���ǵ�����һ��������������ֵ(wrong)
			int minTermsIncluded = newPostingIndex.size();

			Queue<CandidateResult> candidateResultList = new PriorityQueue<CandidateResult>(
			        RETRIEVED_SET_SIZE);

			double threshold = 0.0; /* getminScore(candidateResultList, topk); */

			// int times = 0;
			Score: while (newPostingIndex.size() > 0 && minTermsIncluded > 0)
			{
				// times++;
				// ��ȡ��ǰ��С��ѯ�ʼ����е���С�ĵ�ID��
				// ����Ҫ�����ٰ���һ����ѯ�ʼ�T�в���DocID�����������е�������Ѱ��
				int currentDocIdPostingListIndex = selectMinPostingId(minTermsIncluded);
				IterablePosting currentDocIdPosting = plm
				        .getPosting(newPostingIndex
				                .get(currentDocIdPostingListIndex));
				int currentDocId = currentDocIdPosting.getId();
				CandidateResult currentCandidate = makeCandidateResult(currentDocId);
				// �����Ӧ����newPostingIndex��Index˳����plm�е�˳����Ҫget������ȡ
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
						// ��ǰ�����Ƿ��и�doc
						long docIdfound = iPosting.getId();
						if (docIdfound < currentDocId)
						{
							docIdfound = iPosting.next(currentDocId);
						}
						if (docIdfound == currentDocId)
						{
							// ���
							assignScore(newPostingIndex.get(i),
							        currentCandidate);
						}
						else if (docIdfound == IterablePosting.EOL)
						{
							// ��ǰ���Ƿ��˾�ͷ
							// �����if������û���ô�
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
							// ����ע��i--
							i--;
						}
					}
				}
				// �ڴ�֮ǰҪ�����жϵ�ǰ��postingindex��posting����Ϊ֮ǰ���п��ܸ�posting��ͷ���Ѿ����޳�

				if ((!targetResultSetSizeReached)
				        || currentCandidate.getScore() > threshold)
				{
					// ��������topk��ֵ������δ����
					candidateResultList.add(currentCandidate);
					// ����topk��
					if (candidateResultList.size() > RETRIEVED_SET_SIZE) // ���ڻ��ǵ���
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
				// ���¶�����Ѱ����СDocID
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
			// logger.info("��ʱ��" + timeescaped);
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
			// ����һ��������queryterm's
			// length�������ȶ��У���֤ÿ�����ȼ����ĵ�����С���ĵ����������ѯ�ʶ�Ӧͬһ�ĵ�ʱ�����ȴ���ǰ��Ĳ�ѯ���ĵ�
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
		// ������������ÿ��������������ۼƷ���
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
