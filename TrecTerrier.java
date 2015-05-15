import java.io.IOException;

/*
 * Terrier - Terabyte Retriever 
 * Webpage: http://terrier.org/
 * Contact: terrier{a.}dcs.gla.ac.uk
 * University of Glasgow - School of Computing Science
 * http://www.gla.ac.uk/
 * 
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is TrecTerrier.java
 *
 * The Original Code is Copyright (C) 2004-2014 the University of Glasgow.
 * All Rights Reserved.
 *
 * Contributor(s):
 *   Craig Macdonald <craigm{a.}dcs.gla.ac.uk> (original contributor)
 */

/**
 * Legacy wrapper class for org.terrier.applications.TrecTerrier.
 * 
 * @since 3.0
 */
public class TrecTerrier
{

	/**
	 * Main method - redirects to org.terrier.applications.TrecTerrier.main()
	 * 
	 * @param args
	 *            command line arguments
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException
	{
		System.setProperty("terrier.home", "D:/WorkSpaceJava/myTerrier-4.0");
		for (int i = 0; i <4; i++)
        {
	        org.terrier.applications.TrecTerrier.main(args);
        }
	}
}
