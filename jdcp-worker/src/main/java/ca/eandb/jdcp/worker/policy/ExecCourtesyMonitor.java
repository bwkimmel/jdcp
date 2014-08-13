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

package ca.eandb.jdcp.worker.policy;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * @author Brad
 *
 */
public final class ExecCourtesyMonitor extends PollingCourtesyMonitor {

	private static final Logger logger = Logger.getLogger(ExecCourtesyMonitor.class);

	private final String command;

	private final File workingDirectory;

	public ExecCourtesyMonitor(String command) {
		this(command, null);
	}

	public ExecCourtesyMonitor(String command, File workingDirectory) {
		this.command = command;
		this.workingDirectory = workingDirectory;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PollingCourtesyMonitor#poll()
	 */
	public boolean poll() {
		try {
			Process process = Runtime.getRuntime().exec(command, null,
					workingDirectory);
			while (true) {
				try {
					return process.waitFor() == 0;
				} catch (InterruptedException e) {}
			}
		} catch (IOException e) {
			logger.error("Could not execute courtesy script", e);
			return true;
		}
	}

}
