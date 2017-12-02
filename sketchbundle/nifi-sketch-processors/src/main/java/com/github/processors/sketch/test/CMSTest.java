package com.github.processors.sketch.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.github.processors.sketch.CountMinSketch;
import com.github.processors.sketch.SketchConstants;

public class CMSTest {
	public static final String fileName = "testData.txt";
	public static final String filePath = "src/main/resources";

	public static void main(String[] args) throws FileNotFoundException {
		CountMinSketch.createInstance(SketchConstants.DEFAULT_DELTA, SketchConstants.DEFAULT_EPSILON);
		CountMinSketch c = CountMinSketch.getInstance();

		Scanner scan = new Scanner(
				new File(System.getProperty("user.dir") + File.separator + filePath + File.separator + fileName));
		while (scan.hasNextLine()) {
			String ip = scan.nextLine();
			if (null != ip && !ip.equals("")) {
				c.update(ip);
			}
		}
		/*
		 * c.update("hello"); c.update("world"); c.update("we"); c.update("hello");
		 */
		System.out.println(c.getEstimatedCount("10.0.0.8"));
		System.out.println(c.getEstimatedCount("10.0.0.7"));
		System.out.println(c.getEstimatedCount("10.0.0.9"));
		System.out.println(c.getEstimatedCount("10.0.0.4"));
		System.out.println(c.getEstimatedCount("10.0.0.3"));
		System.out.println(c.getEstimatedCount("10.0.0.6"));
		System.out.println(c.getEstimatedCount("10.0.0.5"));
		System.out.println(c.getEstimatedCount("10.0.0.0"));
		System.out.println(c.getEstimatedCount("10.0.0.2"));
		System.out.println(c.getEstimatedCount("10.0.0.1"));
		System.out.println(c.getEstimatedCount("10.0.0.10"));
		System.out.println(c.getEstimatedCount("10.0.1.1"));
		System.out.println(c.getEstimatedCount("10.1.0.2"));
		System.out.println(c.getEstimatedCount("123"));
		System.out.println(c.getEstimatedCount("10008"));

		/*Actual Results  -- This program produces accurate count results 
		 *  10.0.0.8--9997522
			10.0.0.7--10001593
			10.0.0.9--9996742
			10.0.0.4--10006330
			10.0.0.3--9999548
			10.0.0.6--10001997
			10.0.0.5--10000565
			10.0.0.0--10001306
			10.0.0.2--9992846
			10.0.0.1--10001551
		 * */
		/*
		 * System.out.println(c.getEstimatedCount("hi"));
		 * System.out.println(c.getEstimatedCount("we"));
		 */

		scan.close();
	}

}
