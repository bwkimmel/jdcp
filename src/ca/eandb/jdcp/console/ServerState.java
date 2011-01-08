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

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.prefs.Preferences;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.log4j.Logger;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.server.AuthenticationServer;
import ca.eandb.jdcp.server.JobServer;
import ca.eandb.jdcp.server.classmanager.DbClassManager;
import ca.eandb.jdcp.server.scheduling.PrioritySerialTaskScheduler;
import ca.eandb.jdcp.server.scheduling.TaskScheduler;
import ca.eandb.util.args.CommandArgument;
import ca.eandb.util.args.OptionArgument;
import ca.eandb.util.progress.ProgressState;
import ca.eandb.util.progress.ProgressStateFactory;

/**
 * Provides commands for managing the server.
 * @author Brad Kimmel
 */
public final class ServerState {

	/** The <code>Logger</code> to log messages to. */
	private static final Logger logger = Logger.getLogger(ServerState.class);

	/**
	 * The <code>ProgressMonitor</code>s for tracking the overall progress of
	 * each job running on the server.
	 */
	private List<ProgressState> jobProgressStates = null;

	/** The RMI <code>Registry</code> to register the server with. */
	private Registry registry = null;

	/** The running <code>JobServer</code>. */
	private JobServer jobServer = null;
	
	/** The port that the server is running on. */
	private int port = 0;

	/**
	 * Gets the RMI <code>Registry</code> to register the server with, creating
	 * it if necessary.
	 * @return The RMI <code>Registry</code> to register the server with.
	 * @throws RemoteException If an error occurs while attempting to create
	 * 		the <code>Registry</code>.
	 */
	public synchronized Registry getRegistry(int port) throws RemoteException {
		if (registry == null || port != this.port) {
			registry = LocateRegistry.createRegistry(port);
			this.port = port;
		}
		return registry;
	}

	/**
	 * Removes completed and cancelled jobs from the stat list.
	 */
	@CommandArgument
	public void clean() {
		for (int i = 0; i < jobProgressStates.size();) {
			ProgressState state = jobProgressStates.get(i);
			if (state.isCancelled() || state.isComplete()) {
				jobProgressStates.remove(i);
			} else {
				i++;
			}
		}
	}

	/**
	 * Starts the server.
	 */
	@CommandArgument
	public void start(
			@OptionArgument(value="port", shortKey='P') int port
			) {
		System.out.println("Starting server");
		try {
			
			if (port <= 0) {
				port = JdcpUtil.DEFAULT_PORT;
			}

			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			EmbeddedDataSource ds = new EmbeddedDataSource();
			ds.setConnectionAttributes("create=true");
			ds.setDatabaseName("classes");

			logger.info("Initializing jobProgressStates monitor");
			ProgressStateFactory factory = new ProgressStateFactory();
			jobProgressStates = factory.getProgressStates();

			logger.info("Initializing folders...");
			Preferences pref = Preferences
					.userNodeForPackage(JobServer.class);
			String path = pref.get("rootDirectory", "./server");
			File rootDirectory = new File(path);
			File jobsDirectory = new File(rootDirectory, "jobs");

			rootDirectory.mkdir();
			jobsDirectory.mkdir();

			logger.info("Initializing service");
			DbClassManager classManager = new DbClassManager(ds);
			classManager.prepareDataSource();

			TaskScheduler scheduler = new PrioritySerialTaskScheduler();
			Executor executor = Executors.newCachedThreadPool();
			jobServer = new JobServer(jobsDirectory, factory, scheduler, classManager, executor);
			AuthenticationServer authServer = new AuthenticationServer(jobServer, port);

			logger.info("Binding service");
			Registry registry = getRegistry(port);
			registry.bind("AuthenticationService", authServer);

			logger.info("Server ready");
			System.out.println("Server started");

		} catch (Exception e) {
			System.err.println("Failed to start server");
			logger.error("Failed to start server", e);
		}
	}

	/**
	 * Cancels a job.
	 * @param index The 1-based index of the job to cancel.
	 */
	@CommandArgument
	public void cancel(int index) {
		if (jobProgressStates == null) {
			System.err.println("Server not running");
			return;
		}
		if (index <= 0 || index > jobProgressStates.size()) {
			System.err.println("Invalid job number");
		}
		ProgressState state = jobProgressStates.get(index - 1);
		state.setCancelPending();
	}

	/**
	 * Stops the server.
	 */
	@CommandArgument
	public void stop() {
		try {
			Registry registry = getRegistry(port);
			registry.unbind("AuthenticationService");
			this.jobProgressStates = null;
			System.out.println("Server stopped");
		} catch (Exception e) {
			logger.error("An error occurred while stopping the server", e);
			System.err.println("Server did not shut down cleanly, see log for details.");
		}
	}

	/**
	 * Prints the status of the jobs running on the server.
	 * @param index The 1-based index of the job to print the status of, or
	 * 		zero to print the status of all jobs.
	 */
	@CommandArgument
	public void stat(int index) {
		if (this.jobProgressStates == null) {
			System.err.println("Server not running");
			return;
		}
		if (index == 0) {
			List<ProgressState> progress = new ArrayList<ProgressState>(this.jobProgressStates);
			if (progress != null) {
				System.out.println("   # Title                     Progress Status                          ");
				System.out.println("------------------------------------------------------------------------");
				for (int i = 0, n = progress.size(); i < n; i++) {
					ProgressState state = progress.get(i);
					char flag = ' ';
					if (state.isComplete()) {
						flag = '*';
					} else if (state.isCancelled()) {
						flag = 'X';
					} else if (state.isCancelPending()) {
						flag = 'C';
					}
					String title = state.getTitle();
					if (title.length() > 25) {
						title = title.substring(0, 24) + ">";
					}
					String status = state.getStatus();
					if (status.length() > 32) {
						status = status.substring(0, 31) + ">";
					}
					String progStr = (state.isIndeterminant() ? "????????" : String.format(" % 6.2f%%", 100.0 * state.getProgress()));
					System.out.printf("%c% 3d %-25s %s %-33s\n",
							flag, i + 1, title, progStr, status);
				}
			}
		} else if (index > 0 && index <= this.jobProgressStates.size()) {
			ProgressState state = this.jobProgressStates.get(index - 1);
			System.out.printf("Job #%d", index);
			if (state.isComplete()) {
				System.out.print(" [COMPLETE]");
			} else if (state.isCancelled()) {
				System.out.print(" [CANCELLED]");
			} else if (state.isCancelPending()) {
				System.out.print(" [CANCEL PENDING]");
			}
			System.out.println();
			System.out.printf("Title    : %s\n", state.getTitle());
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
			System.err.println("Invalid job number");
		}
	}

}
