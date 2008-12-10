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

package ca.eandb.jdcp.worker;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.jnlp.BasicService;
import javax.jnlp.ServiceManager;
import javax.jnlp.UnavailableServiceException;
import javax.swing.JDialog;


import ca.eandb.jdcp.concurrent.BackgroundThreadFactory;
import ca.eandb.util.jobs.Job;
import ca.eandb.util.progress.ProgressPanel;

/**
 * @author Brad Kimmel
 *
 */
public final class WorkerMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String host = getParentHost(args);
		int numberOfCpus = Runtime.getRuntime().availableProcessors();
		Executor threadPool = Executors.newFixedThreadPool(numberOfCpus, new BackgroundThreadFactory());
		Job workerJob = new ThreadServiceWorkerJob(host, 10, numberOfCpus, threadPool);

		ProgressPanel monitor = new ProgressPanel();
		monitor.setRootVisible(false);

		final JDialog dialog = new JDialog();
		dialog.addWindowListener(new WindowAdapter() {

			/* (non-Javadoc)
			 * @see java.awt.event.WindowAdapter#windowClosing(java.awt.event.WindowEvent)
			 */
			@Override
			public void windowClosing(WindowEvent e) {
				dialog.setVisible(false);
				System.exit(0);
			}

		});

		dialog.add(monitor);
		dialog.setBounds(0, 0, 400, 300);
		dialog.setVisible(true);

		workerJob.go(monitor);

	}

	/**
	 * Gets the host name to get tasks from.
	 * @param args The command line arguments.
	 * @return The master host.
	 */
	private static String getParentHost(String[] args) {

		/* If a host was passed on the command line, use it. */
		if (args.length > 0) {

			return args[0];

		} else { // args.length == 0

			/*
			 * If this application is being run via Java Web Start, then look
			 * up the code base URL and use that host, otherwise, the parent
			 * is "localhost".
			 */
			try {
				BasicService service = (BasicService) ServiceManager.lookup("javax.jnlp.BasicService");
				return service.getCodeBase().getHost();
			} catch (UnavailableServiceException e) {
				return "localhost";
			}

		}

	}

}
