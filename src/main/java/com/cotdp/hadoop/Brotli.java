package com.cotdp.hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;

/**
 * Created by Yonas on 1/3/16.
 */
public class Brotli{

    Process p;
    InputStream inStream;
    Scanner outputRecords = null;
    public Brotli(File inputFile, int mode) {
    	try {
			inStream = new FileInputStream(inputFile);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        String cmd = "/bin/cat";
        ProcessBuilder pb = new ProcessBuilder();
        pb.redirectOutput();
        pb.command(cmd);
        try {
			p = pb.start();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
    public void printOutput(){
    	if (outputRecords == null){
	    	try {
	    		OutputStream stdin = p.getOutputStream();
	    		IOUtils.copy(inStream, stdin);
	    		inStream.close();
	    		stdin.close();
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	outputRecords = new Scanner(p.getInputStream());
    	}
    	
    	while (outputRecords.hasNextLine()) {
            System.out.println(outputRecords.nextLine());
        }
    }

    public static void main(String[] argv) throws FileNotFoundException{

        FileInputStream fileIn = null;
        FileOutputStream fileOut = null;
        
        Brotli b = new Brotli(new File("/tmp/in.txt"), 0);
        b.printOutput();

    }
}
