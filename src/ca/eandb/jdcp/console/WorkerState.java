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
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
 * @author Brad
 *
 */
public final class WorkerState {

	private static final Logger logger = Logger.getLogger(WorkerState.class);

	private List<ProgressState> taskProgressStates = null;

	private ThreadServiceWorker worker = null;

	private Thread workerThread = null;

	@CommandArgument
	public void start(
			@OptionArgument("ncpus") int numberOfCpus,
			@OptionArgument("host") final String host,
			@OptionArgument("username") final String username,
			@OptionArgument("password") final String password
			) {
		System.out.println("Starting worker with " + Integer.toString(numberOfCpus) + " cpus");

		JobServiceFactory serviceFactory = new JobServiceFactory() {

			public JobService connect() {
				JobService service = null;
				try {
					Registry registry = LocateRegistry.getRegistry(host.isEmpty() ? "localhost" : host);
					AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
					service = auth.authenticate(username.isEmpty() ? "guest" : username, password);
				} catch (NotBoundException e) {
					logger.error("Job service not found at remote host.", e);
				} catch (RemoteException e) {
					logger.error("Could not connect to job service.", e);
				} catch (LoginException e) {
					logger.error("Login failed.", e);
				}
				if (service == null) {
					throw new RuntimeException("No service.");
				}
				return service;
			}

		};

		int availableCpus = Runtime.getRuntime().availableProcessors();
		if (numberOfCpus <= 0 || numberOfCpus > availableCpus) {
			numberOfCpus = availableCpus;
		}
		Executor threadPool = Executors.newFixedThreadPool(numberOfCpus, new BackgroundThreadFactory());

		if (worker != null) {
			logger.info("Shutting down worker");
			worker.shutdown();
			try {
				workerThread.join();
			} catch (InterruptedException e) {
			}
		}

		logger.info("Starting worker");

		ProgressStateFactory factory = new ProgressStateFactory();
		worker = new ThreadServiceWorker(serviceFactory, numberOfCpus,
				threadPool, factory);

		taskProgressStates = factory.getProgressStates();

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

	@CommandArgument
	public void stat() {
		if (taskProgressStates == null) {
			System.out.println("Worker not running");
			return;
		}
		System.out.println("Worker stats");
	}

}
