import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.terrier.matching.daat.Maxscore;

public class Utils
{

	public Utils()
	{
		// TODO Auto-generated constructor stub
	}

	/**
	 * ��singleLineQuery��ʽ���ļ��ж�ȡ����start~end��query��count���� �ֱ�д�벻ͬ���ļ��У�
	 * Ĭ��·������D�̸�Ŀ¼��
	 * 
	 * @param filepath
	 * @param start
	 * @param end
	 * @param count
	 * @throws IOException
	 */
	public void classify(String filepath, int start, int end, int count)
	        throws IOException
	{
		HashMap<BufferedWriter, Integer> bwWithCounter = new HashMap<BufferedWriter, Integer>();
		// ���ɳ���1~20��query���ϣ�����Ϊ������
		for (int i = start; i <= end; i++)
		{
			bwWithCounter.put(new BufferedWriter(new FileWriter(new File(
			        "d:/query_" + i + ".txt"))), i);
		}

		List<Map.Entry<BufferedWriter, Integer>> lbw = new ArrayList<Map.Entry<BufferedWriter, Integer>>(
		        bwWithCounter.entrySet());
		Collections.sort(lbw,
		        new Comparator<Map.Entry<BufferedWriter, Integer>>() {

			        @Override
			        public int compare(Entry<BufferedWriter, Integer> o1,
			                Entry<BufferedWriter, Integer> o2)
			        {
				        return o1.getValue() - o2.getValue();
			        }
		        });
		for (int i = 0; i < lbw.size(); i++)
		{
			lbw.get(i).setValue(1);
			// ����Ĭ��ÿ�����ȵ�queryֻ�ռ�100�������Բ��㣬�����ᳬ��
			lbw.get(i).getKey().write(String.valueOf(count));
			lbw.get(i).getKey().newLine();
		}
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		String line = br.readLine();
		while (line != null && line.length() != 0)
		{
			String[] terms = line.split(":")[1].split(" ");
			// if (terms.length>10)
			// {
			// line = br.readLine();
			// continue;
			// }
			Entry<BufferedWriter, Integer> entry = lbw.get(terms.length - 1);
			// �ռ���100������д��
			if (entry.getValue() == count + 1)
			{
				line = br.readLine();
				continue;
			}
			entry.getKey().write(entry.getValue() + ":");
			entry.setValue(entry.getValue() + 1);
			for (int i = 0; i < terms.length; i++)
			{
				entry.getKey().write(terms[i] + " ");
			}
			entry.getKey().newLine();
			line = br.readLine();
		}

		br.close();
		for (Entry<BufferedWriter, Integer> entry : lbw)
		{
			entry.getKey().flush();
			entry.getKey().close();
		}
	}

	public void mergeQueries(String filepath) throws IOException
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(filepath
		        + "all.txt"));
		bw.write("900");
		bw.newLine();
		for (int i = 2; i < 11; i++)
		{
			BufferedReader br = new BufferedReader(new FileReader(filepath
			        + "query_" + i + ".txt"));
			br.readLine();// ������һ��
			String line = br.readLine();
			int j = 1;
			while (line != null)
			{
				String newline = String.valueOf((i - 2) * 100 + j++)
				        + line.substring(line.indexOf(":"), line.length());
				bw.write(newline);
				bw.newLine();
				line = br.readLine();
			}
			br.close();
			bw.flush();
		}
		bw.close();
	}

	public void extractResultForDAAT(String dirPath) throws IOException
	{
		File dir = new File(dirPath);
		File[] results = dir.listFiles();
		BufferedWriter bwAIO = new BufferedWriter(new FileWriter(new File(
		        dirPath + "DAAT.csv")));
		for (int i = 0; i < results.length; i++)
		{
			BufferedReader br = new BufferedReader(new FileReader(results[i]));
			// ֻȡ���һ�εĽ����һ�����Ĵ�
			// ��"time to intialise index"Ϊ���
			String line;
			final String MARK = "time to intialise index";
			final CharSequence csMARK = MARK.subSequence(0, MARK.length());
			final String NAME = "query_";
			final CharSequence csNAME = NAME.substring(0, NAME.length());
			final String TIME = "Time to process query";
			final CharSequence csTIME = TIME.subSequence(0, TIME.length());
			int count = 0;
			while (true)
			{
				line = br.readLine();
				if (line.contains(csMARK))
				{
					count++;
					if (count == 4)
						break;
				}
			}
			// Ѱ���ļ���
			while (!line.contains(csNAME))
				line = br.readLine();
			String[] elements = line.split("[/ ]");
			String outname = null;
			for (String string : elements)
				if (string.contains(csNAME))
					outname = string;
			String queryLength = outname.split("[_.]")[1];
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
			        dirPath + outname)));
			// Ѱ��ʱ���в����
			while (line != null)
			{
				if (line.contains(csTIME))
				{
					String[] temp = line.substring(line.indexOf(TIME),
					        line.length()).split("[: ]");
					double tmptime = Double.valueOf(temp[temp.length - 1]) * 1000;
					// �������
					// format: "DAAT $QUERY_LENGTH $TIME"
					bw.write("DAAT," + queryLength + ","
					        + String.valueOf(tmptime));
					bw.newLine();

					bwAIO.write("DAAT," + queryLength + ","
					        + String.valueOf(tmptime));
					bwAIO.newLine();
					// �������
					// bw.write(temp[temp.length - 1]+" ");
				}
				line = br.readLine();
			}
			br.close();
			bw.flush();
			bw.close();
		}
		bwAIO.flush();
		bwAIO.close();
	}

	public void extractResultForDAATFromOne(String dirPath, BufferedWriter bwall)
	        throws IOException
	{
		File dir = new File(dirPath);
		File[] results = dir.listFiles();
		BufferedWriter bwAIO = new BufferedWriter(new FileWriter(new File(
		        dirPath + "DAAT.csv")));
		for (int i = 0; i < results.length; i++)
		{
			BufferedReader br = new BufferedReader(new FileReader(results[i]));
			// ֻȡ���һ�εĽ����һ�����Ĵ�
			// ��"time to intialise index"Ϊ���
			String line;
			final String MARK = "time to intialise index";
			final CharSequence csMARK = MARK.subSequence(0, MARK.length());
			final String TIME = "Time to process query";
			final CharSequence csTIME = TIME.subSequence(0, TIME.length());
			int count = 0;
			while (true)
			{
				line = br.readLine();
				if (line.contains(csMARK))
				{
					count++;
					if (count == 4)
						break;
				}
			}
			int queryLength = 2;
			int time = 1;
			// Ѱ��ʱ���в����
			while (line != null)
			{
				if (line.contains(csTIME))
				{
					String[] temp = line.substring(line.indexOf(TIME),
					        line.length()).split("[: ]");
					double tmptime = Double.valueOf(temp[temp.length - 1]) * 1000;
					// �������
					// format: "DAAT $QUERY_LENGTH $TIME"
					if (time == 101)
					{
						time = 1;
						queryLength++;
					}
					bwAIO.write("DAAT," + queryLength + ","
					        + String.valueOf(tmptime));
					bwAIO.newLine();
					bwall.write("DAAT," + queryLength + ","
					        + String.valueOf(tmptime));
					bwall.newLine();
					time++;
				}
				line = br.readLine();
			}
			br.close();
		}
		bwAIO.flush();
		bwAIO.close();
	}

	public void extractResultForDPFromOne(String dirPath, final String dpType,
	        BufferedWriter bwall) throws IOException
	{
		File dir = new File(dirPath);
		File[] results = dir.listFiles();
		BufferedWriter bwAIO = new BufferedWriter(new FileWriter(new File(
		        dirPath + dpType + ".csv")));
		for (int i = 0; i < results.length; i++)
		{
			BufferedReader br = new BufferedReader(new FileReader(results[i]));
			// ֻȡ���һ�εĽ����һ�����Ĵ�
			// ��"time to intialise index"Ϊ���
			String line;
			final String MARK = "time to intialise index";
			final CharSequence csMARK = MARK.subSequence(0, MARK.length());
			final String TIME = "Time to process query";
			final CharSequence csTIME = TIME.subSequence(0, TIME.length());
			final String WASTE = "time wasted";
			final CharSequence csWASTE = WASTE.subSequence(0, WASTE.length());
			int count = 0;
			while (true)
			{
				line = br.readLine();
				if (line.contains(csMARK))
				{
					count++;
					if (count == 4)
						break;
				}
			}
			// Ѱ��ʱ���в����
			double wastetime = 0.0;
			int queryLength = 2;
			int time = 1;
			while (line != null)
			{
				if (line.contains(csWASTE))
				{
					String[] temp = line.substring(line.indexOf(WASTE),
					        line.length()).split("[: ]");
					wastetime = Double.valueOf(temp[temp.length - 1]);
					line = br.readLine();
					continue;
				}
				if (line.contains(csTIME))
				{
					String[] temp = line.substring(line.indexOf(TIME),
					        line.length()).split("[: ]");
					double tmptime = (Double.valueOf(temp[temp.length - 1]) - wastetime) * 1000;
					// �������
					// format:"$DP_TYPE $QUERY_LENGTH $TIME"
					if (time == 101)
					{
						time = 1;
						queryLength++;
					}
					bwAIO.write(dpType + "," + queryLength + ","
					        + String.valueOf(tmptime));
					bwAIO.newLine();
					bwall.write(dpType + "," + queryLength + ","
					        + String.valueOf(tmptime));
					bwall.newLine();
					wastetime = 0.0;
					time++;
				}
				line = br.readLine();
			}
			br.close();
		}
		bwAIO.flush();
		bwAIO.close();
	}

	public void extractResultForDP(String dirPath, final String dpType)
	        throws IOException
	{
		File dir = new File(dirPath);
		File[] results = dir.listFiles();
		BufferedWriter bwAIO = new BufferedWriter(new FileWriter(new File(
		        dirPath + dpType + ".csv")));
		for (int i = 0; i < results.length; i++)
		{
			BufferedReader br = new BufferedReader(new FileReader(results[i]));
			// ֻȡ���һ�εĽ����һ�����Ĵ�
			// ��"time to intialise index"Ϊ���
			String line;
			final String MARK = "time to intialise index";
			final CharSequence csMARK = MARK.subSequence(0, MARK.length());
			final String NAME = "query_";
			final CharSequence csNAME = NAME.substring(0, NAME.length());
			final String TIME = "Time to process query";
			final CharSequence csTIME = TIME.subSequence(0, TIME.length());
			final String WASTE = "time wasted";
			final CharSequence csWASTE = WASTE.subSequence(0, WASTE.length());
			int count = 0;
			while (true)
			{
				line = br.readLine();
				if (line.contains(csMARK))
				{
					count++;
					if (count == 4)
						break;
				}
			}
			// Ѱ���ļ���
			while (!line.contains(csNAME))
				line = br.readLine();
			String[] elements = line.split("[/ ]");
			String outname = null;
			for (String string : elements)
				if (string.contains(csNAME))
					outname = string;
			String queryLength = outname.split("[_.]")[1];
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
			        dirPath + outname)));
			// Ѱ��ʱ���в����
			double wastetime = 0.0;
			while (line != null)
			{
				if (line.contains(csWASTE))
				{
					String[] temp = line.substring(line.indexOf(WASTE),
					        line.length()).split("[: ]");
					wastetime = Double.valueOf(temp[temp.length - 1]);
					line = br.readLine();
					continue;
				}
				if (line.contains(csTIME))
				{
					String[] temp = line.substring(line.indexOf(TIME),
					        line.length()).split("[: ]");
					double tmptime = (Double.valueOf(temp[temp.length - 1]) - wastetime) * 1000;
					// �������
					// format:"$DP_TYPE $QUERY_LENGTH $TIME"
					bw.write(dpType + "," + queryLength + ","
					        + String.valueOf(tmptime));
					bw.newLine();
					bwAIO.write(dpType + "," + queryLength + ","
					        + String.valueOf(tmptime));
					bwAIO.newLine();
					// �������
					// bw.write(String.valueOf(Double
					// .valueOf(temp[temp.length - 1]) - wastetime)
					// + " ");
					wastetime = 0.0;
				}
				line = br.readLine();
			}
			br.close();
			bw.flush();
			bw.close();
		}
		bwAIO.flush();
		bwAIO.close();
	}

	public void mergeResults(String path) throws IOException
	{
		BufferedWriter bwall = new BufferedWriter(new FileWriter(path
		        + "result.csv"));
		bwall.write("type,length,time");
		bwall.newLine();
		extractResultForDAATFromOne(path + "DAAT/", bwall);
		extractResultForDPFromOne(path + "Maxscore/", "Maxscore", bwall);
		extractResultForDPFromOne(path + "WAND/", "WAND", bwall);
		bwall.flush();
		bwall.close();
	}

	public static void main(String[] args) throws IOException
	{
		// TODO Auto-generated method stub
		// ���ֲ�ѯ��
		// new Utils().classify("D:/WorkSpaceJava/05.efficiency_topics", 1, 20,
		// 100);
		// new Utils().mergeIntoOne("D:/a/");
		// �ռ�DAAT
		// new Utils().extractResultForDAAT("d:/result/DAAT/");
		// �ռ�Maxscore
		// new Utils().extractResultForDP("d:/result/Maxscore/", "Maxscore");
		// �ռ�WAND
		// new Utils().extractResultForDP("d:/result/WAND/", "WAND");

		// new Utils().extractResultForDAATFromOne("d:/result/DAAT/");
		// new Utils()
		// .extractResultForDPFromOne("d:/result/Maxscore/", "Maxscore");
		// new Utils().extractResultForDPFromOne("d:/result/WAND/", "WAND");
		new Utils().mergeResults("d:/result/");
	}

}
