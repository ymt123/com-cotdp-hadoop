/**
 * 
 * Adapated from com.cotdp.hadoop Copyright 2011 Michael Cutler <m@cotdp.com>
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

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BrotliFileRecordReader extends RecordReader<Text,Text>{
	/** InputStream used to read the ZIP file from the FileSystem */
	private FSDataInputStream fsin;

	/** Brotli file parser/decompresser */
	private Process decompressor;

	/** Uncompressed file name */
	private Text currentKey = new Text();

	/** Uncompressed file contents */
	private Text currentValue = new Text();

	/** Used to indicate progress */
	private boolean isFinished = false;

	private Scanner outputLines;
	/**
	 * Initialize and open the ZIP file from the FileSystem
	 */
	@Override
	public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
			throws IOException, InterruptedException
	{
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = taskAttemptContext.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem( conf );
		
		// Set the file path as the key
		currentKey.set(path.getName());
		// Open the stream
		fsin = fs.open( path );
		
		String cmd = "/bin/cat";
        ProcessBuilder pb = new ProcessBuilder();
        pb.redirectOutput();
        pb.command(cmd);
        
        try {
        	decompressor = pb.start();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		if (outputLines == null){
			// First time through
			currentKey.set(""); // TODO: Fix This, should be filename?
			outputLines = new Scanner(decompressor.getInputStream());
		}
		
		if (outputLines.hasNextLine()){
			currentValue.set(outputLines.nextLine());
			return true;
		}else{
			// At the end of the end of the file
			isFinished = true;
			return false;
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
		
		try { fsin.close(); } catch ( Exception ignore ) { }
	}

}
