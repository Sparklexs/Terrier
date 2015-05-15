package org.terrier.applications;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.terrier.sorting.HeapSortInt;
import org.terrier.structures.CollectionStatistics;
import org.terrier.structures.DocumentIndex;
import org.terrier.structures.FSOMapFileLexicon;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.bit.BitPostingIndex;
import org.terrier.structures.postings.IterablePosting;
import org.terrier.structures.postings.bit.FieldIterablePosting;
import org.terrier.utility.Files;

public class FilesForDualSorted
{
	IndexOnDisk	index;
	String	    prefix;

	public FilesForDualSorted(String _prefix)
	{
		// TODO Auto-generated constructor stub
		System.setProperty("terrier.home", "D:/WorkSpaceJava/myTerrier-4.0");
		// System.setProperty("terrier.home",
		// "F:/Song/WorkSpaceJava/myTerrier-4.0");
		index = Index.createIndex();
		// 确定目的路径存在
		Files.mkdir("./DualSorted");
		prefix = _prefix;
	}

	public void generateStatistic() throws IOException
	{
		CollectionStatistics cs = index.getCollectionStatistics();
		DataOutputStream csDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".statistic"));
		csDos.writeBytes("AverageDocumentLength:"
		        + cs.getAverageDocumentLength() + "\n");
		csDos.writeBytes("NumberOfDocuments:" + cs.getNumberOfDocuments()
		        + "\n");
		csDos.writeBytes("NumberOfPointers:" + cs.getNumberOfPointers() + "\n");
		csDos.writeBytes("NumberOfTokens:" + cs.getNumberOfTokens() + "\n");
		csDos.writeBytes("NumberOfUniqueTerms:" + cs.getNumberOfUniqueTerms()
		        + "\n");
		csDos.flush();
		csDos.close();
	}

	public void generateDocLengthFile() throws IOException
	{
		// 获取DocumentIndex
		DocumentIndex doi = index.getDocumentIndex();

		DataOutputStream docLengthDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".doclength"));

		int docnum = doi.getNumberOfDocuments();
		docLengthDos.writeBytes(String.valueOf(docnum) + "\n");

		for (int i = 0; i < docnum; i++)
		{
			docLengthDos.writeBytes(String.valueOf(i) + " "
			        + String.valueOf(doi.getDocumentLength(i)) + "\n");
		}
		docLengthDos.flush();
		docLengthDos.close();
		System.err.println("Doc Length File Finished!");
	}

	/**
	 * 该方法是按照docID的增序生成的invlist和对应的invlistfreq
	 * 
	 * @throws IOException
	 */
	public void generateWordsFileByDocID() throws IOException
	{
		// 生成三个待写入的文件

		DataOutputStream wordsDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".words"));
		DataOutputStream InvDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlist"));
		DataOutputStream InvFreqDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlistfreq"));

		// 获取对应的索引
		FSOMapFileLexicon lexicon = (FSOMapFileLexicon) index.getLexicon();
		BitPostingIndex invIndex = (BitPostingIndex) index.getInvertedIndex();

		Iterator<Map.Entry<String, LexiconEntry>> lexIterator = lexicon
		        .iterator();

		// 写入每一个term，和term对应的posting list
		while (lexIterator.hasNext())
		{
			Map.Entry<String, LexiconEntry> entry = lexIterator.next();
			LexiconEntry lexEntry = entry.getValue();

			// 写入term
			wordsDos.writeBytes(entry.getKey() + "\n");

			int documentFreq = lexEntry.getDocumentFrequency();
			System.out.println("now writing term \"" + entry.getKey()
			        + "\" with " + documentFreq + " entries.");

			// 首先写入每个posting list的长度
			InvDos.writeBytes(String.valueOf(documentFreq) + " ");
			InvFreqDos.writeBytes(String.valueOf(documentFreq) + " ");

			FieldIterablePosting fip = (FieldIterablePosting) invIndex
			        .getPostings(lexEntry);

			// 之后写入posting中的docid及frequency
			int docID = fip.next();
			while (docID != IterablePosting.EOL)
			{
				InvDos.writeBytes(String.valueOf(docID) + " ");
				InvFreqDos.writeBytes(String.valueOf(fip.getFrequency()) + " ");
				docID = fip.next();
			}
			// 写完一个list换行
			InvDos.writeChar('\n');
			InvFreqDos.writeChar('\n');
		}
		// 关闭流
		wordsDos.flush();
		wordsDos.close();
		InvDos.flush();
		InvDos.close();
		InvFreqDos.flush();
		InvFreqDos.close();

		System.err.println("all finished!");
	}

	/**
	 * 该方法是按照freq的降序生成的invlist和对应的invlistfreq
	 * 
	 * @throws IOException
	 */
	public void generateWordsFileByTF() throws IOException
	{
		// 生成三个待写入的文件

		DataOutputStream wordsDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".words"));
		DataOutputStream InvDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlist"));
		DataOutputStream InvFreqDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlistfreq"));

		// 获取对应的索引
		FSOMapFileLexicon lexicon = (FSOMapFileLexicon) index.getLexicon();
		BitPostingIndex invIndex = (BitPostingIndex) index.getInvertedIndex();

		Iterator<Map.Entry<String, LexiconEntry>> lexIterator = lexicon
		        .iterator();

		// 写入每一个term，和term对应的posting list
		while (lexIterator.hasNext())
		{
			Map.Entry<String, LexiconEntry> entry = lexIterator.next();
			LexiconEntry lexEntry = entry.getValue();

			// 写入term
			wordsDos.writeBytes(entry.getKey() + "\n");

			int documentFreq = lexEntry.getDocumentFrequency();
			System.out.println("now writing term \"" + entry.getKey()
			        + "\" with " + documentFreq + " entries.");

			// 首先写入每个posting list的长度
			InvDos.writeBytes(String.valueOf(documentFreq) + " ");
			InvFreqDos.writeBytes(String.valueOf(documentFreq) + " ");

			FieldIterablePosting fip = (FieldIterablePosting) invIndex
			        .getPostings(lexEntry);

			// 之后写入posting中的docid及frequency
			int docID = fip.next();

			int[] idArray = new int[documentFreq];
			int[] freqArray = new int[documentFreq];
			int i = 0;
			while (docID != IterablePosting.EOL)
			{
				// 重点在这里和ByDocID不同，其他一样
				idArray[i] = docID;
				freqArray[i] = fip.getFrequency();
				i++;
				docID = fip.next();

				// InvDos.writeBytes(String.valueOf(docID) + " ");
				// InvFreqDos.writeBytes(String.valueOf(fip.getFrequency()) +
				// " ");
				// docID = fip.next();
			}
			HeapSortInt.descendingHeapSort(freqArray, idArray);
			for (int j = 0; j < freqArray.length; j++)
			{
				InvDos.writeBytes(String.valueOf(idArray[j] + " "));
				InvFreqDos.writeBytes(String.valueOf(freqArray[j] + " "));
			}
			// 写完一个list换行
			InvDos.writeChar('\n');
			InvFreqDos.writeChar('\n');
		}
		// 关闭流
		wordsDos.flush();
		wordsDos.close();
		InvDos.flush();
		InvDos.close();
		InvFreqDos.flush();
		InvFreqDos.close();

		System.err.println("all finished!");
	}

	public static void main(String[] args) throws IOException
	{
		// TODO Auto-generated method stub
		FilesForDualSorted fds = new FilesForDualSorted("test");
		fds.generateStatistic();
		// fds.generateDocLengthFile();
		// fds.generateWordsFileByTF();
	}

}
