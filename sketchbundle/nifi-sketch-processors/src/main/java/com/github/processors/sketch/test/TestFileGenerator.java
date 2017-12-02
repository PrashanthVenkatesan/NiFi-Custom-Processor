package com.github.processors.sketch.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class TestFileGenerator {
	public static final String fileName = "testData.txt";
	public static final String filePath = "src/main/resources";

	public static void main(String[] args) {
		// To generate a random number
		Random r = new Random();
		BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			fw = new FileWriter(System.getProperty("user.dir") + File.separator + filePath + File.separator + fileName);
			bw = new BufferedWriter(fw);

			Map<String, Integer> IPMap = new HashMap<String, Integer>();
			String baseIP = "10.0.0.";
			for (int i = 0; i < 100000000; i++) {
				int lastByte = r.nextInt(10);
				String IP = baseIP + lastByte;
				if (!IPMap.containsKey(IP))
					IPMap.put(IP, 1);
				else {
					IPMap.put(IP, IPMap.get(IP) + 1);
				}
				bw.write(IP + "\n");
			}

			for (Entry<String, Integer> e : IPMap.entrySet()) {
				System.out.println(e.getKey() + "--" + e.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
}
