/*
 * Copyright (c) 2008 Bradley W. Kimmel
 * 
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package ca.eandb.jdcp.server.classmanager;

import java.io.File;
import java.nio.ByteBuffer;

import ca.eandb.util.StringUtil;

/**
 * @author brad
 *
 */
public final class TestFileClassManager {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		printBytes(StringUtil.hexToByteArray("0123456789ABCDEF"));

		File root = new File("C:\\test");
		FileClassManager cm = new FileClassManager(root);


		cm.setClassDefinition("Test1", StringUtil.hexToByteArray("0123456789ABCDEF"));
		printBytes(cm.getClassDefinition("Test1"));
		printBytes(cm.getClassDigest("Test1"));

		ClassManager child = cm.createChildClassManager();
		cm.setClassDefinition("Test1", StringUtil.hexToByteArray("FEDCBA9876543210"));
		cm.setClassDefinition("Test1", StringUtil.hexToByteArray("FEDCBA9876543210"));
		printBytes(cm.getClassDefinition("Test1"));
		printBytes(cm.getClassDigest("Test1"));
		printBytes(child.getClassDefinition("Test1"));
		printBytes(child.getClassDigest("Test1"));

		cm.releaseChildClassManager(child);

		child.getClassDefinition("Test1");

	}

	private static void printBytes(byte[] bytes) {
		System.out.println(StringUtil.toHex(bytes));
	}

	private static void printBytes(ByteBuffer bytes) {
		byte[] array = new byte[bytes.remaining()];
		bytes.get(array);
		printBytes(array);
	}

}