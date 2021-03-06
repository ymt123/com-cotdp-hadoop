/**
 * Copyright 2011 Michael Cutler <m@cotdp.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cotdp.hadoop;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This RecordReader implementation extracts individual files from a ZIP
 * file and hands them over to the Mapper. The "key" is the decompressed
 * file name, the "value" is the file contents.
 */
public class ZipFileRecordReader
extends RecordReader<Text, Text>
{
	/** InputStream used to read the ZIP file from the FileSystem */
	private FSDataInputStream fsin;

	/** ZIP file parser/decompresser */
	private ZipInputStream zip;

	/** Uncompressed file name */
	private Text currentKey = new Text();

	/** Uncompressed file contents */
	private Text currentValue = new Text();

	/** Used to indicate progress */
	private boolean isFinished = false;
	private boolean startedReading = false;

	private BufferedReader br;

	/**
	 * Initialise and open the ZIP file from the FileSystem
	 */
	@Override
	public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
			throws IOException, InterruptedException
	{
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = taskAttemptContext.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem( conf );

		// Open the stream
		fsin = fs.open( path );
		zip = new ZipInputStream( fsin );
	}

	/**
	 * This is where the magic happens, each ZipEntry is decompressed and
	 * readied for the Mapper. The contents of each file is held *in memory*
	 * in a BytesWritable object.
	 * 
	 * If the ZipFileInputFormat has been set to Lenient (not the default),
	 * certain exceptions will be gracefully ignored to prevent a larger job
	 * from failing.
	 */
	@Override
	public boolean nextKeyValue()
			throws IOException, InterruptedException
	{
		String currentLine;
		if (startedReading == true && (currentLine = br.readLine() )!= null) {
			// Reading already opened file
			currentValue.set(currentLine);
			return true;
		}else{
			// Open a new file
			if (startedReading){
				zip.closeEntry();
			}
			ZipEntry entry = null;
			try
			{
				while ((entry = zip.getNextEntry()) != null && entry.isDirectory());
			}
			catch ( ZipException e )
			{
				if ( ZipFileInputFormat.getLenient() == false )
					throw e;
			}

			// Sanity check
			if ( entry == null )
			{
				isFinished = true;
				return false;
			}

			// Filename
			currentKey.set(entry.getName());

			InputStreamReader isr = new InputStreamReader(zip);
			br = new BufferedReader(isr);
			currentLine = br.readLine();
			currentValue.set(currentLine);
			startedReading = true;
			return true;
		}
	}

	/**
	 * Rather than calculating progress, we just keep it simple
	 */
	@Override
	public float getProgress()
			throws IOException, InterruptedException
	{
		return isFinished ? 1 : 0;
	}

	/**
	 * Returns the current key (name of the zipped file)
	 */
	@Override
	public Text getCurrentKey()
			throws IOException, InterruptedException
	{
		return currentKey;
	}

	/**
	 * Returns the current value (contents of the zipped file)
	 */
	@Override
	public Text getCurrentValue()
			throws IOException, InterruptedException
	{
		return currentValue;
	}

	/**
	 * Close quietly, ignoring any exceptions
	 */
	@Override
	public void close()
			throws IOException
	{
		try { zip.close(); } catch ( Exception ignore ) { }
		try { fsin.close(); } catch ( Exception ignore ) { }
	}
}
