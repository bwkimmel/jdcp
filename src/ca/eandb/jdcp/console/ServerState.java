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

import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.prefs.Preferences;

import javax.swing.JFrame;

import org.apache.derby.jdbc.EmbeddedDataSource;

import ca.eandb.jdcp.server.AuthenticationServer;
import ca.eandb.jdcp.server.JobServer;
import ca.eandb.jdcp.server.classmanager.DbClassManager;
import ca.eandb.jdcp.server.scheduling.PrioritySerialTaskScheduler;
import ca.eandb.jdcp.server.scheduling.TaskScheduler;
import ca.eandb.util.args.CommandArgument;
import ca.eandb.util.progress.ProgressPanel;
import ca.eandb.util.progress.ProgressState;
import ca.eandb.util.progress.ProgressStateFactory;

/**
 * @author Brad
 *
 */
public final class ServerState {

	private List<ProgressState> progress = null;

	private Registry registry = null;

	public synchronized Registry getRegistry() throws RemoteException {
		if (registry == null) {
			registry = LocateRegistry.createRegistry(1099);
		}
		return registry;
	}

	@CommandArgument
	public void start() {
		System.out.println("Starting server");
		try {

			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			EmbeddedDataSource ds = new EmbeddedDataSource();
			ds.setConnectionAttributes("create=true");
			ds.setDatabaseName("classes");

			System.err.print("Initializing progress monitor...");
			ProgressStateFactory factory = new ProgressStateFactory();
			progress = factory.getProgressStates();
			System.err.println("OK");

			System.err.print("Initializing folders...");
			Preferences pref = Preferences
					.userNodeForPackage(JobServer.class);
			String path = pref.get("rootDirectory", "./server");
			File rootDirectory = new File(path);
			File jobsDirectory = new File(rootDirectory, "jobs");

			rootDirectory.mkdir();
			jobsDirectory.mkdir();
			System.err.println("OK");

			System.err.print("Initializing service...");
			DbClassManager classManager = new DbClassManager(ds);
			classManager.prepareDataSource();

			TaskScheduler scheduler = new PrioritySerialTaskScheduler();
			Executor executor = Executors.newCachedThreadPool();
			JobServer jobServer = new JobServer(jobsDirectory, factory, scheduler, classManager, executor);
			AuthenticationServer authServer = new AuthenticationServer(jobServer);
			System.err.println("OK");

			System.err.print("Binding service...");
			Registry registry = getRegistry();
			registry.bind("AuthenticationService", authServer);
			System.err.println("OK");

			System.err.println("Server ready");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@CommandArgument
	public void stop() {
		System.out.println("Stopping server");
		try {
			Registry registry = getRegistry();
			registry.unbind("AuthenticationService");
			this.progress = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@CommandArgument
	public void stat() {
		if (this.progress == null) {
			System.out.println("Server not running");
			return;
		}
		System.out.println("Server stats");
		List<ProgressState> progress = new ArrayList<ProgressState>(this.progress);
		if (progress != null) {
			System.out.println("   # Title                     Progress Status                          ");
			System.out.println("------------------------------------------------------------------------");
			for (int i = 0, n = progress.size(); i < n; i++) {
				ProgressState state = progress.get(i);
				char flag = state.isCancelPending() ? '*' : ' ';
				String title = state.getTitle();
				if (title.length() > 25) {
					title = title.substring(0, 24) + ">";
				}
				String status = state.getStatus();
				if (status.length() > 32) {
					status = status.substring(0, 31) + ">";
				}
				double pct = 100.0 * state.getProgress();
				System.out.printf("%c% 3d %25s % 6.2f%% %33s\n",
						flag, i, title, pct, status);
			}
		}
	}

}
