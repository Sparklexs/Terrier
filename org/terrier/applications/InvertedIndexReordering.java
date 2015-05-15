package org.terrier.applications;

import gnu.trove.TIntArrayList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.terrier.compression.bit.BitOutputStream;
import org.terrier.sorting.HeapSortInt;
import org.terrier.structures.BasicLexiconEntry;
import org.terrier.structures.DocumentIndex;
import org.terrier.structures.DocumentIndexEntry;
import org.terrier.structures.FSOMapFileLexicon;
import org.terrier.structures.FSOMapFileLexiconOutputStream;
import org.terrier.structures.FieldLexiconEntry;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.MetaIndex;
import org.terrier.structures.bit.BitPostingIndex;
import org.terrier.structures.indexing.CompressingMetaIndexBuilder;
import org.terrier.structures.postings.IterablePosting;
import org.terrier.structures.postings.bit.FieldIterablePosting;
import org.terrier.structures.seralization.FixedSizeTextFactory;
import org.terrier.utility.ApplicationSetup;
import org.terrier.utility.Files;

public class InvertedIndexReordering
{
	protected ArrayList<String>	fileNames	= new ArrayList<String>();
	final int	                DOCS_NUM_THRESHOLD;

	class PostingTuple
	{
		int	docid;
		int	tf;
		int	fieldtf1;
		int	fieldtf2;

		public PostingTuple(int _docid, int _tf, int _fieldtf1, int _fieldtf2)
		{
			docid = _docid;
			tf = _tf;
			fieldtf1 = _fieldtf1;
			fieldtf2 = _fieldtf2;
		}
	}

	class PostingPointer
	{
		int		        pos;
		PostingTuple	currentTuple;

		DataInputStream	dis;

		public PostingPointer(int indexOfFile) throws IOException
		{
			pos = 0;
			dis = new DataInputStream(Files.openFileStream(fileNames
			        .get(indexOfFile)));
			currentTuple = new PostingTuple(-1, -1, -1, -1);
		}

		public PostingTuple nextTuple() throws IOException
		{
			if (dis.available() > 0)
			{
				int docid = dis.readInt();
				int tf = dis.readInt();
				int fieldtf1 = dis.readInt();
				int fieldtf2 = dis.readInt();
				pos++;
				currentTuple = new PostingTuple(docid, tf, fieldtf1, fieldtf2);
				return currentTuple;

			}
			else
				return null;
		}

		public int getPos()
		{
			return pos;
		}

	}

	IndexOnDisk	index;

	public InvertedIndexReordering()
	{
		this(200000);
	}

	public InvertedIndexReordering(int threshold)
	{
		System.setProperty("terrier.home", "D:/WorkSpaceJava/myTerrier-4.0");
		index = Index.createIndex();
		DOCS_NUM_THRESHOLD = threshold;
	}

	public void ReorderDocumentAndMeta() throws IOException
	{
		DataInputStream datain = new DataInputStream(
		        Files.openFileStream("./var/index/reorder.Alp_Seq.txt"));
		// 获取DocumentIndex
		DocumentIndex doi = index.getDocumentIndex();
		// doi.getDocumentEntry(0)
		int docs = doi.getNumberOfDocuments();
		// 获取metaIndex
		MetaIndex metaIndex = index.getMetaIndex();

		// 设置fsarrayfile和meta file的writer

		DataOutputStream dos = new DataOutputStream(new FileOutputStream(
		        "./var/index/data.reorder.document.fsarrayfile"));

		final String[] forwardMetaKeys = ApplicationSetup.getProperty(
		        "indexer.meta.forward.keys", "docno").split("\\s*,\\s*");
		final int[] metaKeyLengths = parseInts(ApplicationSetup.getProperty(
		        "indexer.meta.forward.keylens", "20").split("\\s*,\\s*"));
		final String[] reverseMetaKeys = ApplicationSetup.getProperty(
		        "indexer.meta.reverse.keys", "").split("\\s*,\\s*");
		CompressingMetaIndexBuilder mb = new CompressingMetaIndexBuilder(index,
		        "reorder.meta", forwardMetaKeys, metaKeyLengths,
		        reverseMetaKeys);

		for (int i = 0; i < docs; i++)
		{
			// 寻找修改后的docID，并写入
			int newdocid = datain.readInt();
			DocumentIndexEntry die = doi.getDocumentEntry(newdocid);
			die.write(dos);
			String[] test = metaIndex.getAllItems(newdocid);
			mb.writeDocumentEntry(test);
		}
		dos.flush();
		dos.close();
		mb.closeWithoutflush();
		datain.close();
	}

	public void ReorderInverted() throws IOException
	{
		// 获取Inverted Index
		BitPostingIndex invIndex = (BitPostingIndex) index.getInvertedIndex();

		// 获取Lexicon
		FSOMapFileLexicon lexicon = (FSOMapFileLexicon) index.getLexicon();

		Iterator<Map.Entry<String, LexiconEntry>> lexIterator = lexicon
		        .iterator();

		// 每次都要重新初始化datain，方便从头读取
		DataInputStream datain = new DataInputStream(
		        Files.openFileStream("./var/index/reorder.Seq_Alp.txt"));

		int[] seq2Alt = new int[index.getEnd() + 1];
		for (int i = 0; i < seq2Alt.length; i++)
			seq2Alt[i] = datain.readInt();
		datain.close();

		// 声明输出文件
		BitOutputStream invertedStream = new BitOutputStream(
		        "./var/index/data.reorder.inverted.bf");
		FSOMapFileLexiconOutputStream lexStream = new FSOMapFileLexiconOutputStream(
		        index, "reorder.lexicon", new FixedSizeTextFactory(20),
		        FieldLexiconEntry.Factory.class);

		byte bitoffset = invertedStream.getBitOffset();
		long byteoffset = invertedStream.getByteOffset();

		// 外层循环遍历所有lexicon term
		while (lexIterator.hasNext())
		{
			Map.Entry<String, LexiconEntry> entry = lexIterator.next();

			String termString = entry.getKey();
			LexiconEntry lexEntry = entry.getValue();

			System.out.println("now processing: " + lexEntry.getTermId()
			        + " \"" + termString + "\" with "
			        + lexEntry.getNumberOfEntries() + " entries.");

			int currentDocNum = lexEntry.getDocumentFrequency();
			int loop = (int) Math.ceil((double) currentDocNum
			        / DOCS_NUM_THRESHOLD);
			if (loop == 0)
				System.err.println("no posting list???");

			// 这里默认只有两个field
			TIntArrayList ids = new TIntArrayList();
			TIntArrayList tf = new TIntArrayList();
			TIntArrayList fieldtf1 = new TIntArrayList();
			TIntArrayList fieldtf2 = new TIntArrayList();

			// 内层循环每个term遍历所有posting
			FieldIterablePosting fip = (FieldIterablePosting) invIndex
			        .getPostings(lexEntry);

			int docID = fip.next();
			int lastDocID = -1;
			int newid = 0;

			if (loop == 1)
			{
				while (docID != IterablePosting.EOL)
				{
					// 写入数组，上限是10,485,761
					// 根据原来的docID寻找新的docID
					newid = seq2Alt[docID];

					ids.add(newid);
					tf.add(fip.getFrequency());
					fieldtf1.add(fip.getFieldFrequencies()[0]);
					fieldtf2.add(fip.getFieldFrequencies()[1]);

					docID = fip.next();
				}

				// 一个term的postingList遍历结束，排序
				int[] idsA = ids.toNativeArray();
				int[] tfA = tf.toNativeArray();
				int[] fieldtf1A = fieldtf1.toNativeArray();
				int[] fieldtf2A = fieldtf2.toNativeArray();

				HeapSortInt.ascendingHeapSort(idsA, tfA, fieldtf1A, fieldtf2A);

				for (int j = 0; j < fieldtf2A.length; j++)
				{
					invertedStream.writeGamma(idsA[j] - lastDocID);
					invertedStream.writeUnary(tfA[j]);
					invertedStream.writeUnary(fieldtf1A[j] + 1);
					invertedStream.writeUnary(fieldtf2A[j] + 1);

					lastDocID = idsA[j];
				}
			}
			else
			{
				for (int i = 0; i < loop; i++)
				{
					int length = i == loop - 1 ? currentDocNum
					        - DOCS_NUM_THRESHOLD * i : DOCS_NUM_THRESHOLD;

					for (int j = 0; j < length; j++)
					{
						// 由于外层循环的控制，不会取到eol，那么等于-1就只有个开头一种情况
						newid = seq2Alt[docID];
						ids.add(newid);
						tf.add(fip.getFrequency());
						fieldtf1.add(fip.getFieldFrequencies()[0]);
						fieldtf2.add(fip.getFieldFrequencies()[1]);

						docID = fip.next();
					}

					// 第i次run完成，写入临时文件
					int[] idsA = ids.toNativeArray();
					int[] tfA = tf.toNativeArray();
					int[] fieldtf1A = fieldtf1.toNativeArray();
					int[] fieldtf2A = fieldtf2.toNativeArray();

					ids.clear();
					tf.clear();
					fieldtf1.clear();
					fieldtf2.clear();

					HeapSortInt.ascendingHeapSort(idsA, tfA, fieldtf1A,
					        fieldtf2A);

					DataOutputStream dos = new DataOutputStream(
					        Files.writeFileStream(new File("./var/index/"
					                + "merge.tmp" + i + ".txt")));
					fileNames.add("./var/index/" + "merge.tmp" + i + ".txt");

					for (int j = 0; j < fieldtf2A.length; j++)
					{
						dos.writeInt(idsA[j]);
						dos.writeInt(tfA[j]);
						dos.writeInt(fieldtf1A[j]);
						dos.writeInt(fieldtf2A[j]);
					}
					dos.flush();
					dos.close();
				}
				// 合并入一个bf文件
				Queue<PostingPointer> queue = new PriorityQueue<PostingPointer>(
				        fileNames.size(), new Comparator<PostingPointer>() {
					        @Override
					        public int compare(PostingPointer o1,
					                PostingPointer o2)
					        {
						        return o1.currentTuple.docid
						                - o2.currentTuple.docid;
					        }
				        });
				// 初始化所有指针
				PostingPointer[] pointers = new PostingPointer[fileNames.size()];
				for (int i = 0; i < fileNames.size(); i++)
				{
					pointers[i] = new PostingPointer(i);
					pointers[i].nextTuple();
					queue.add(pointers[i]);
				}

				while (!queue.isEmpty())
				{
					PostingPointer curr = queue.poll();
					invertedStream.writeGamma(curr.currentTuple.docid
					        - lastDocID);
					invertedStream.writeUnary(curr.currentTuple.tf);
					invertedStream.writeUnary(curr.currentTuple.fieldtf1 + 1);
					invertedStream.writeUnary(curr.currentTuple.fieldtf2 + 1);

					lastDocID = curr.currentTuple.docid;

					if (curr.nextTuple() != null)
						queue.add(curr);
					else
						curr.dis.close();
				}

				for (int i = 0; i < fileNames.size(); i++)
					if (!Files.delete(fileNames.get(i)))
						System.err.println(fileNames.get(i));

				fileNames.clear();
			}

			((BasicLexiconEntry) lexEntry).setOffset(byteoffset, bitoffset);
			bitoffset = invertedStream.getBitOffset();
			byteoffset = invertedStream.getByteOffset();
			lexStream.writeNextEntry(termString, lexEntry);
		}
		invertedStream.close();
		lexStream.closeWithoutFlush();
		index.close();
		// }}
	}

	protected static final int[] parseInts(String[] in)
	{
		final int l = in.length;
		final int[] rtr = new int[l];
		for (int i = 0; i < l; i++)
			rtr[i] = Integer.parseInt(in[i]);
		return rtr;
	}

	public boolean ReplaceFile()
	{
		boolean isFinished = false;
		String tmpPath = "./var/index/data.";

		isFinished = Files.delete(tmpPath + "inverted.bf");
		isFinished = Files.rename(tmpPath + "reorder.inverted.bf", tmpPath
		        + "inverted.bf");

		isFinished = Files.delete(tmpPath + "lexicon.fsomapfile");
		isFinished = Files.rename(tmpPath + "reorder.lexicon.fsomapfile",
		        tmpPath + "lexicon.fsomapfile");

		isFinished = Files.delete(tmpPath + "document.fsarrayfile");
		isFinished = Files.rename(tmpPath + "reorder.document.fsarrayfile",
		        tmpPath + "document.fsarrayfile");

		isFinished = Files.delete(tmpPath + "meta.idx");
		isFinished = Files.rename(tmpPath + "reorder.meta.idx", tmpPath
		        + "meta.idx");

		isFinished = Files.delete(tmpPath + "meta.zdata");
		isFinished = Files.rename(tmpPath + "reorder.meta.zdata", tmpPath
		        + "meta.zdata");
		return isFinished;
	}

	public static void main(String[] args) throws Exception
	{

		// PreReorderWorkIndisk my = new PreReorderWorkIndisk(
		// "./etc/collection.spec", "TrecDocTags", 250000);
		//
		// my.DoWork();

		InvertedIndexReordering ir = new InvertedIndexReordering(250000);
		ir.ReorderDocumentAndMeta();
		ir.ReorderInverted();
		if (!ir.ReplaceFile())
		{
			throw new Exception("Index Replace Failed!");
		}
		System.err.println("Finish!");
	}
}
