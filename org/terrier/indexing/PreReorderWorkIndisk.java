package org.terrier.indexing;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

import org.terrier.sorting.HeapSortInt;
import org.terrier.utility.Files;

/**
 * 该类继承自PreReorderWork，当doc数目太大时，写入临时文件中合并
 * 该类的作用仅是按照collections.spec文件指定的collection逐个读取出每个DOC，
 * 并提取出URL并双向排序
 * 因为DocumentIndex是按照docID顺序写的，
 * 那么重写就需要根据重排后的newDocID寻找之前的docID进行写入
 * 所以需要ALP to SEQ
 * 而Inverted Index则是读取出原来的docID，替换成newDocID
 * 所以需要SEQ to ALP
 * @author John
 *
 */
public class PreReorderWorkIndisk extends PreReorderWork
{
	class FilePointer
	{
		int		        pos;
		IdURLTuple		currentTuple;

		DataInputStream	dis;

		public FilePointer(int indexOfFile) throws IOException
		{
			pos = 0;
			dis = new DataInputStream(Files.openFileStream(fileNames
			        .get(indexOfFile)));
			currentTuple = new IdURLTuple(-1, "");
		}

		public IdURLTuple nextTuple() throws IOException
		{
			if (dis.available() > 0)
			{
				String url = dis.readUTF();
				int docid = dis.readInt();
				pos++;
				currentTuple = new IdURLTuple(docid, url);
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

	class IdURLTuple
	{
		public int		docid;
		public String	url;

		public IdURLTuple(int _docid, String _url)
		{
			docid = _docid;
			url = _url;
		}
	}

	class SeqAlpTuple
	{
		public int	seq;
		public int	alp;

		public SeqAlpTuple(int _seq, int _alp)
		{
			seq = _seq;
			alp = _alp;
		}
	}

	class seqPointer
	{
		int		        pos;
		SeqAlpTuple		currentTuple;
		DataInputStream	dis;

		public seqPointer(int indexOfFile) throws IOException
		{
			pos = 0;
			dis = new DataInputStream(Files.openFileStream(fileNames
			        .get(indexOfFile)));
			currentTuple = new SeqAlpTuple(-1, -1);
		}

		public SeqAlpTuple nextTuple() throws IOException
		{
			if (dis.available() > 0)
			{
				int seq = dis.readInt();
				int alp = dis.readInt();
				pos++;
				currentTuple = new SeqAlpTuple(seq, alp);
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

	/**
	 * alp2Seq表示index[key]=value，key表示字母序的排序，value表示 出现的顺序
	 * seq2Alp表示index[key]=value,key表示出现的顺序，value表示字 母序的顺序
	 */
	int	                        numDocuments	= 0;
	final int	                DOCS_NUM_THRESHOLD;
	final int[]	                TEMPLATE_ARRAY;

	protected ArrayList<String>	fileNames	 = new ArrayList<String>();

	public PreReorderWorkIndisk(String CollectionSpecFilename, String TagSet,
	        int docslimit) throws IOException
	{
		super(CollectionSpecFilename, TagSet);

		DOCS_NUM_THRESHOLD = docslimit;
		TEMPLATE_ARRAY = new int[DOCS_NUM_THRESHOLD];

		for (int i = 0; i < TEMPLATE_ARRAY.length; i++)
			TEMPLATE_ARRAY[i] = i;
	}

	public PreReorderWorkIndisk(String CollectionSpecFilename, String TagSet)
	        throws IOException
	{
		this(CollectionSpecFilename, TagSet, 250000);
	}

	public void addOneURL(StringBuilder docHDRBuilder) throws IOException
	{
		if (docHDRBuilder != null)
		{
			String uRLString = docHDRBuilder.toString().split(" ")[0];
			urllist.add(uRLString);

			if (urllist.size() == DOCS_NUM_THRESHOLD)
				flushTmpFileToDisk();
		}
		else
			System.out.println("End of the collection, Time to merge!");
	}

	public void flushTmpFileToDisk() throws IOException
	{
		String[] strArray = urllist.toArray(new String[0]);
		int length = urllist.size();
		numDocuments += length;
		int times = fileNames.size();
		int[] alp2Seq;
		alp2Seq = Arrays.copyOf(TEMPLATE_ARRAY, length);

		HeapSortInt.ascendingHeapSort(strArray, alp2Seq);

		DataOutputStream dos = new DataOutputStream(
		        Files.writeFileStream(new File(path + "tmp" + times + ".txt")));
		fileNames.add(path + "tmp" + times + ".txt");

		for (int i = 0; i < length; i++)
		{
			dos.writeUTF(strArray[i]);
			dos.writeInt(alp2Seq[i] + DOCS_NUM_THRESHOLD * times);
		}
		dos.flush();
		dos.close();

		this.urllist.clear();
	}

	public void mergeAll() throws IOException
	{
		Queue<FilePointer> queue = new PriorityQueue<FilePointer>(
		        fileNames.size(), new Comparator<FilePointer>() {
			        @Override
			        public int compare(FilePointer o1, FilePointer o2)
			        {
				        return o1.currentTuple.url
				                .compareToIgnoreCase(o2.currentTuple.url);
			        }
		        });

		// 初始化所有指针
		FilePointer[] pointers = new FilePointer[fileNames.size()];
		for (int i = 0; i < fileNames.size(); i++)
		{
			pointers[i] = new FilePointer(i);
			pointers[i].nextTuple();
			queue.add(pointers[i]);
		}

		while (!queue.isEmpty())
		{
			FilePointer curr = queue.poll();
			alp2SeqStream.writeInt(curr.currentTuple.docid);

			if (curr.nextTuple() != null)
				queue.add(curr);
			else
				curr.dis.close();

		}
		alp2SeqStream.flush();
		alp2SeqStream.close();
		for (int i = 0; i < fileNames.size(); i++)
		{
			if (!Files.delete(fileNames.get(i)))
			{
				System.err.println(fileNames.get(i));
			}
		}
	}

	/**
	 * 该函数的作用是在获取了全局的AlpToSeq后，重新按照Seq顺序排列一遍，以获得原来的docid顺序下新的排序
	 * 
	 * @throws IOException
	 */
	public void remapFile() throws IOException
	{
		int loop = fileNames.size();
		fileNames.clear();
		DataInputStream alp2SeqIn = new DataInputStream(
		        Files.openFileStream(path + "Alp_Seq.txt"));

		for (int j = 0; j < loop; j++)
		{
			int length = j == (loop - 1) ? numDocuments - DOCS_NUM_THRESHOLD
			        * j : DOCS_NUM_THRESHOLD;
			int[] alp = Arrays.copyOf(TEMPLATE_ARRAY, length);
			int[] seq2Alp = Arrays.copyOf(TEMPLATE_ARRAY, length);

			for (int i = 0; i < length; i++)
			{
				seq2Alp[i] = alp2SeqIn.readInt();
			}
			HeapSortInt.ascendingHeapSort(seq2Alp, alp);

			DataOutputStream dos = new DataOutputStream(
			        Files.writeFileStream(new File(path + "tmp" + j + ".txt")));
			fileNames.add(path + "tmp" + j + ".txt");

			for (int i = 0; i < length; i++)
			{
				dos.writeInt(seq2Alp[i]);
				dos.writeInt(alp[i] + j * DOCS_NUM_THRESHOLD);
			}
			dos.flush();
			dos.close();
		}

		// 开始合并
		Queue<seqPointer> queue = new PriorityQueue<seqPointer>(
		        fileNames.size(), new Comparator<seqPointer>() {
			        @Override
			        public int compare(seqPointer o1, seqPointer o2)
			        {
				        return o1.currentTuple.seq - o2.currentTuple.seq;
			        }
		        });
		seqPointer[] pointers = new seqPointer[fileNames.size()];
		for (int i = 0; i < fileNames.size(); i++)
		{
			pointers[i] = new seqPointer(i);
			pointers[i].nextTuple();
			queue.add(pointers[i]);
		}

		while (!queue.isEmpty())
		{
			seqPointer curr = queue.poll();
			seq2AlpStream.writeInt(curr.currentTuple.alp);

			if (curr.nextTuple() != null)
				queue.add(curr);
			else
				curr.dis.close();
		}
		seq2AlpStream.flush();
		seq2AlpStream.close();
		for (int i = 0; i < fileNames.size(); i++)
			if (!Files.delete(fileNames.get(i)))
				System.err.println(fileNames.get(i));
	}

	public void DoWork() throws IOException
	{
		while (collection.hasNext())
		{
			addOneURL(getDocHDR());
		}
		if (urllist.size() > 0)
		{
			flushTmpFileToDisk();
		}
		mergeAll();
		remapFile();

		Files.copyFile(path + "Alp_Seq.txt", "./var/index/reorder.Alp_Seq.txt");
		Files.copyFile(path + "Seq_Alp.txt", "./var/index/reorder.Seq_Alp.txt");

		System.out.println("finish!");
	}

	public static void main(String[] args) throws IOException
	{
		PreReorderWorkIndisk my = new PreReorderWorkIndisk(
		        "./etc/collection.spec", "TrecDocTags", 50000);
		my.DoWork();

	}

}
