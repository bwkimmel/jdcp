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

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.jnlp.BasicService;
import javax.jnlp.ServiceManager;
import javax.jnlp.UnavailableServiceException;
import javax.swing.JDialog;
import javax.swing.SwingUtilities;

import ca.eandb.jdcp.concurrent.BackgroundThreadFactory;
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
		final String host = getParentHost(args);

		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				start(host);
			}
		});
	}

	private static void start(String host) {

		int numberOfCpus = Runtime.getRuntime().availableProcessors();
		Executor threadPool = Executors.newFixedThreadPool(numberOfCpus, new BackgroundThreadFactory());
		ProgressPanel panel = new ProgressPanel();

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

		Container contentPane = dialog.getContentPane();
		contentPane.setLayout(new BorderLayout());
		contentPane.add(panel, BorderLayout.CENTER);
		dialog.setBounds(0, 0, 400, 300);
		dialog.pack();
		dialog.validate();

		dialog.setVisible(true);

		Runnable worker = new ThreadServiceWorker(host, 10, numberOfCpus, threadPool, panel);
		Thread thread = new Thread(worker);

		thread.start();

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
