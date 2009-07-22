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

package ca.eandb.jdcp.console;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import javax.security.auth.login.LoginException;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.log4j.Logger;

import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.worker.JobServiceFactory;
import ca.eandb.jdcp.worker.ThreadServiceWorker;
import ca.eandb.util.args.CommandArgument;
import ca.eandb.util.args.OptionArgument;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.progress.ProgressState;
import ca.eandb.util.progress.ProgressStateFactory;

/**
 * Provides commands for managing a worker process.
 * @author Brad Kimmel
 */
public final class WorkerState {

	/** The <code>Logger</code> to use to log messages. */
	private static final Logger logger = Logger.getLogger(WorkerState.class);

	/** The interval between connection attempts (in seconds). */
	private static final int RECONNECT_INTERVAL = 60;

	/** The list of <code>ProgressMonitor</code>s for each worker thread. */
	private List<ProgressState> taskProgressStates = null;

	/**
	 * The <code>ThreadServiceWorker</code> that manages the worker threads.
	 */
	private ThreadServiceWorker worker = null;

	/**
	 * The <code>Thread</code> on which the <code>ThreadServiceWorker</code>
	 * executes.
	 */
	private Thread workerThread = null;

	/**
	 * The number of seconds until the next reconnection attempt is made.
	 */
	private int reconnectCountdown = -1;

	/**
	 * Starts the worker process.
	 * @param numberOfCpus The number of worker threads to spawn.
	 * @param host The name of the host to connect to.
	 * @param username The user name to log in with.
	 * @param password The password to log in with.
	 */
	@CommandArgument
	public void start(
			@OptionArgument("ncpus") int numberOfCpus,
			@OptionArgument("host") final String host,
			@OptionArgument("username") final String username,
			@OptionArgument("password") final String password
			) {

		int availableCpus = Runtime.getRuntime().availableProcessors();
		if (numberOfCpus <= 0 || numberOfCpus > availableCpus) {
			numberOfCpus = availableCpus;
		}
		System.out.println("Starting worker with " + Integer.toString(numberOfCpus) + " cpus");

		if (worker != null) {
			logger.info("Shutting down worker");
			worker.shutdown();
			try {
				workerThread.join();
			} catch (InterruptedException e) {
			}
		}

		logger.info("Starting worker");

		JobServiceFactory serviceFactory = new JobServiceFactory() {
			public JobService connect() {
				return waitForService(
						host.equals("") ? "localhost" : host,
						username.equals("") ? "guest" : username,
						password, RECONNECT_INTERVAL);
			}
		};

		ThreadFactory threadFactory = new BackgroundThreadFactory();
		ProgressStateFactory monitorFactory = new ProgressStateFactory();
		worker = new ThreadServiceWorker(serviceFactory, threadFactory, monitorFactory);
		worker.setMaxWorkers(numberOfCpus);

		taskProgressStates = monitorFactory.getProgressStates();

		logger.info("Preparing data source");

		EmbeddedDataSource ds = null;
		try {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			ds = new EmbeddedDataSource();
			ds.setConnectionAttributes("create=true");
			ds.setDatabaseName("classes");
			worker.setDataSource(ds);
		} catch (ClassNotFoundException e) {
			logger.error("Could not locate database driver.", e);
		} catch (SQLException e) {
			logger.error("Error occurred while initializing data source.", e);
		}

		workerThread = new Thread(worker);
		workerThread.start();

	}

	private JobService waitForService(String host, String username, String password, int retryInterval) {
		JobService service = null;
		while (true) {
			reconnectCountdown = 0;
			service = connect(host, username, password);
			if (service != null) {
				break;
			}

			for (int i = retryInterval; i > 0; i--) {
				reconnectCountdown = i;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					reconnectCountdown = -1;
					return null;
				}
			}
		}
		reconnectCountdown = -1;
		return service;
	}

	private JobService connect(String host, String username, String password) {
		JobService service = null;
		try {
			Registry registry = LocateRegistry.getRegistry(host, 5327);
			AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
			service = auth.authenticate(username, password);
		} catch (NotBoundException e) {
			logger.error("Job service not found at remote host.", e);
		} catch (RemoteException e) {
			logger.error("Could not connect to job service.", e);
		} catch (LoginException e) {
			logger.error("Login failed.", e);
		}
		return service;
	}

	/**
	 * Sets the maximum number of concurrent workers.
	 * @param numberOfCpus The number of CPUs to use (zero to use all available
	 * 		CPUs on the machine).
	 */
	@CommandArgument
	public void setcpus(int numberOfCpus) {
		if (worker == null) {
			System.err.println("Worker not running.");
			return;
		}
		int availableCpus = Runtime.getRuntime().availableProcessors();
		if (numberOfCpus <= 0 || numberOfCpus > availableCpus) {
			numberOfCpus = availableCpus;
		}
		System.out.printf("Setting number of CPUs to %d\n", numberOfCpus);
		worker.setMaxWorkers(numberOfCpus);
	}

	/**
	 * Stops the worker process.
	 */
	@CommandArgument
	public void stop() {
		System.out.println("Stopping worker");
		worker.shutdown();
		workerThread.interrupt();
		try {
			workerThread.join();
		} catch (InterruptedException e) {
			logger.warn("Joining to worker thread interrupted", e);
		}
		worker = null;
		workerThread = null;
		taskProgressStates = null;
	}

	/**
	 * Prints the status of the worker threads.
	 * @param index The 1-based index of the worker thread to print the status
	 * 		of, or zero to print the status of all worker threads.
	 */
	@CommandArgument
	public void stat(int index) {
		if (reconnectCountdown > 0) {
			System.out.printf("Lost connection, reconnecting in %d seconds.\n", reconnectCountdown);
			return;
		}
		if (reconnectCountdown == 0) {
			System.out.println("Connecting...");
			return;
		}
		if (taskProgressStates == null) {
			System.out.println("Worker not running");
			return;
		}
		Iterator<ProgressState> iter = taskProgressStates.iterator();
		while (iter.hasNext()) {
			ProgressState state = iter.next();
			if (state.isCancelled() || state.isComplete()) {
				iter.remove();
			}
		}
		if (index == 0) { // print status of all workers.
			List<ProgressState> taskProgressStates = new ArrayList<ProgressState>(this.taskProgressStates);
			if (taskProgressStates != null) {
				System.out.println("  # Progress                         Status                             ");
				System.out.println("------------------------------------------------------------------------");
				for (int i = 0, n = taskProgressStates.size(); i < n; i++) {
					ProgressState state = taskProgressStates.get(i);
					char flag = ' ';
					if (state.isComplete()) {
						flag = '*';
					} else if (state.isCancelled()) {
						flag = 'X';
					} else if (state.isCancelPending()) {
						flag = 'C';
					}
					String status = state.getStatus();
					if (status.length() > 35) {
						status = status.substring(0, 34) + ">";
					}
					boolean indeterminant = state.isIndeterminant();
					double progress = state.getProgress();
					String progressBar;
					if (!indeterminant) {
						StringBuilder progressBarBuilder = new StringBuilder("|");
						for (int j = 0; j < 25; j++) {
							if (indeterminant) {
							}
							progressBarBuilder.append((progress >= (double) (j + 1) / 25.0) ? "=" : " ");
						}
						progressBarBuilder.append("|");
						progressBar = progressBarBuilder.toString();
					} else {
						progressBar = "|?????????????????????????|";
					}
					String progStr = (indeterminant ? "????" : String.format("% 3.0f%%", 100.0 * progress));
					System.out.printf("%c% 2d %s %s %-35s\n",
							flag, i + 1, progressBar, progStr, status);
				}
			}
		} else if (index > 0 && index <= this.taskProgressStates.size()) {
			// print status of a single worker.
			ProgressState state = this.taskProgressStates.get(index - 1);
			System.out.printf("Worker #%d", index);
			if (state.isComplete()) {
				System.out.print(" [COMPLETE]");
			} else if (state.isCancelled()) {
				System.out.print(" [CANCELLED]");
			} else if (state.isCancelPending()) {
				System.out.print(" [CANCEL PENDING]");
			}
			System.out.println();
			if (state.isIndeterminant()) {
				System.out.print("Progress : ???");
			} else {
				System.out.printf("Progress : %.2f%%", 100.0 * state.getProgress());
			}
			int maximum = state.getMaximum();
			int value = state.getValue();
			if (maximum > 0) {
				System.out.printf(" (%d/%d)", value, maximum);
			}
			System.out.println();
			System.out.printf("Status   : %s\n", state.getStatus());
		} else {
			System.err.println("Invalid worker number");
		}

	}

}
