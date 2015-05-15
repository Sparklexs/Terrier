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
		// ȷ��Ŀ��·������
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
		// ��ȡDocumentIndex
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
	 * �÷����ǰ���docID���������ɵ�invlist�Ͷ�Ӧ��invlistfreq
	 * 
	 * @throws IOException
	 */
	public void generateWordsFileByDocID() throws IOException
	{
		// ����������д����ļ�

		DataOutputStream wordsDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".words"));
		DataOutputStream InvDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlist"));
		DataOutputStream InvFreqDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlistfreq"));

		// ��ȡ��Ӧ������
		FSOMapFileLexicon lexicon = (FSOMapFileLexicon) index.getLexicon();
		BitPostingIndex invIndex = (BitPostingIndex) index.getInvertedIndex();

		Iterator<Map.Entry<String, LexiconEntry>> lexIterator = lexicon
		        .iterator();

		// д��ÿһ��term����term��Ӧ��posting list
		while (lexIterator.hasNext())
		{
			Map.Entry<String, LexiconEntry> entry = lexIterator.next();
			LexiconEntry lexEntry = entry.getValue();

			// д��term
			wordsDos.writeBytes(entry.getKey() + "\n");

			int documentFreq = lexEntry.getDocumentFrequency();
			System.out.println("now writing term \"" + entry.getKey()
			        + "\" with " + documentFreq + " entries.");

			// ����д��ÿ��posting list�ĳ���
			InvDos.writeBytes(String.valueOf(documentFreq) + " ");
			InvFreqDos.writeBytes(String.valueOf(documentFreq) + " ");

			FieldIterablePosting fip = (FieldIterablePosting) invIndex
			        .getPostings(lexEntry);

			// ֮��д��posting�е�docid��frequency
			int docID = fip.next();
			while (docID != IterablePosting.EOL)
			{
				InvDos.writeBytes(String.valueOf(docID) + " ");
				InvFreqDos.writeBytes(String.valueOf(fip.getFrequency()) + " ");
				docID = fip.next();
			}
			// д��һ��list����
			InvDos.writeChar('\n');
			InvFreqDos.writeChar('\n');
		}
		// �ر���
		wordsDos.flush();
		wordsDos.close();
		InvDos.flush();
		InvDos.close();
		InvFreqDos.flush();
		InvFreqDos.close();

		System.err.println("all finished!");
	}

	/**
	 * �÷����ǰ���freq�Ľ������ɵ�invlist�Ͷ�Ӧ��invlistfreq
	 * 
	 * @throws IOException
	 */
	public void generateWordsFileByTF() throws IOException
	{
		// ����������д����ļ�

		DataOutputStream wordsDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".words"));
		DataOutputStream InvDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlist"));
		DataOutputStream InvFreqDos = new DataOutputStream(
		        Files.writeFileStream("./DualSorted/" + prefix + ".invlistfreq"));

		// ��ȡ��Ӧ������
		FSOMapFileLexicon lexicon = (FSOMapFileLexicon) index.getLexicon();
		BitPostingIndex invIndex = (BitPostingIndex) index.getInvertedIndex();

		Iterator<Map.Entry<String, LexiconEntry>> lexIterator = lexicon
		        .iterator();

		// д��ÿһ��term����term��Ӧ��posting list
		while (lexIterator.hasNext())
		{
			Map.Entry<String, LexiconEntry> entry = lexIterator.next();
			LexiconEntry lexEntry = entry.getValue();

			// д��term
			wordsDos.writeBytes(entry.getKey() + "\n");

			int documentFreq = lexEntry.getDocumentFrequency();
			System.out.println("now writing term \"" + entry.getKey()
			        + "\" with " + documentFreq + " entries.");

			// ����д��ÿ��posting list�ĳ���
			InvDos.writeBytes(String.valueOf(documentFreq) + " ");
			InvFreqDos.writeBytes(String.valueOf(documentFreq) + " ");

			FieldIterablePosting fip = (FieldIterablePosting) invIndex
			        .getPostings(lexEntry);

			// ֮��д��posting�е�docid��frequency
			int docID = fip.next();

			int[] idArray = new int[documentFreq];
			int[] freqArray = new int[documentFreq];
			int i = 0;
			while (docID != IterablePosting.EOL)
			{
				// �ص��������ByDocID��ͬ������һ��
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
			// д��һ��list����
			InvDos.writeChar('\n');
			InvFreqDos.writeChar('\n');
		}
		// �ر���
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
