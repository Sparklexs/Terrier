package org.terrier.indexing;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.terrier.sorting.HeapSortInt;
import org.terrier.utility.Files;
import org.terrier.utility.TagSet;

/**
 * 最初的原型类，全部在内存中操作
 * 
 * @author John
 *
 */
public class PreReorderWork
{
	static
	{
		System.setProperty("terrier.home", "D:/WorkSpaceJava/myTerrier-4.0");
	}
	TRECCollection	     collection;

	private final char[]	start_dochdrTag;
	private final char[]	end_dochdrTag;

	DataOutputStream	 alp2SeqStream;

	DataOutputStream	 seq2AlpStream;

	ArrayList<String>	 urllist	= new ArrayList<String>();
	String	             path	 = "./reorder/reorder.";

	public PreReorderWork(String CollectionSpecFilename, String TagSet)
	        throws IOException
	{
		collection = new TRECCollection(CollectionSpecFilename, TagSet, null,
		        null);
		TagSet tagset = new TagSet(TagSet);

		String tmpstart_dochdrTag = "<" + tagset.getTagsToSkip() + ">";
		String tmpend_dochdrTag = "</" + tagset.getTagsToSkip() + ">";

		start_dochdrTag = tmpstart_dochdrTag.toCharArray();
		end_dochdrTag = tmpend_dochdrTag.toCharArray();

		/**
		 * alp2Seq表示index[key]=value，key表示字母序的排序，value表示 出现的顺序
		 * seq2Alp表示index[key]=value,key表示出现的顺序，value表示字 母序的顺序
		 */
		alp2SeqStream = new DataOutputStream(Files.writeFileStream(path
		        + "Alp_Seq.txt"));
		seq2AlpStream = new DataOutputStream(Files.writeFileStream(path
		        + "Seq_Alp.txt"));
	}

	public StringBuilder getDocHDR() throws IOException
	{
		StringBuilder sb = collection.getTag(start_dochdrTag.length,
		        start_dochdrTag, end_dochdrTag);
		if (sb != null)
			return sb.deleteCharAt(0);
		else
			return null;

	}

	public static void main(String[] args) throws IOException
	{
		PreReorderWork my = new PreReorderWork("./etc/collection.spec",
		        "TrecDocTags");

		// FileWriter fWriter = new FileWriter(benchmarkFile);

		while (my.collection.hasNext())
		{
			// 以下为读取docHDR
			StringBuilder docHDRBuilder = my.getDocHDR();
			if (docHDRBuilder != null)
			{
				String uRLString = docHDRBuilder.toString().split(" ")[0];
				my.urllist.add(uRLString);
			}
			// 以下为读取docNO和docHDR
			/*
			 * StringBuilder dOCIDBuilder = my.collection.getTag(
			 * my.collection.start_docnoTagLength, my.collection.start_docnoTag,
			 * my.collection.end_docnoTag); StringBuilder docHDRBuilder; if
			 * (my.collection.hasNext()) { docHDRBuilder =
			 * my.collection.getTag(my.start_dochdrTag.length,
			 * my.start_dochdrTag, my.end_dochdrTag);
			 * 
			 * String docIDString = dOCIDBuilder.toString(); String uRLString =
			 * docHDRBuilder.toString().split(" ")[0]; strlist.add(uRLString);
			 * // fWriter.write(docIDString + " " + uRLString + "\n"); }
			 */
		}
		String[] strArray = my.urllist.toArray(new String[0]);

		int[] alp2Seq = new int[strArray.length];

		for (int i = 0; i < alp2Seq.length; i++)
		{
			alp2Seq[i] = i;
		}
		int[] seq2Alp = Arrays.copyOf(alp2Seq, strArray.length);

		HeapSortInt.ascendingHeapSort(strArray, alp2Seq);

		for (int i = 0; i < alp2Seq.length; i++)
		{
			my.alp2SeqStream.writeInt(alp2Seq[i]);
			// my.alp2SeqStream.writeUTF(strArray[i]);
		}

		HeapSortInt.ascendingHeapSort(alp2Seq, seq2Alp);

		my.alp2SeqStream.close();

		// alp2SeqStream=new BitByteOutputStream(seq2AlpFile);
		for (int i = 0; i < seq2Alp.length; i++)
		{
			my.seq2AlpStream.writeInt(seq2Alp[i]);
			// fWriter.write((i) + ":" + index[i] + ":" + strArray[i] + "\n");
		}
		// int i = 1;
		//
		// while (listIterator.hasNext())
		// {
		// fWriter.write((i++) + ":" + listIterator.next()+ "\n");
		// }
		// fWriter.flush();
		// fWriter.close();
		my.seq2AlpStream.close();

		Files.copyFile(my.path + "Alp_Seq.txt",
		        "./var/index/reorder.Alp_Seq.txt");
		Files.copyFile(my.path + "Seq_Alp.txt",
		        "./var/index/reorder.Seq_Alp.txt");

		System.out.println("finish!");

	}

}
