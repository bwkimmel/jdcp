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

import java.awt.AWTException;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.MenuShortcut;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.awt.TrayIcon.MessageType;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.concurrent.ThreadFactory;
import java.util.prefs.Preferences;

import javax.security.auth.login.LoginException;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.text.Document;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.ProtocolVersionException;
import ca.eandb.jdcp.worker.policy.CourtesyMonitor;
import ca.eandb.jdcp.worker.policy.CourtesyMonitorFactory;
import ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor;
import ca.eandb.jdcp.worker.policy.UnconditionalCourtesyMonitor;
import ca.eandb.util.args.ArgumentProcessor;
import ca.eandb.util.args.BooleanFieldOption;
import ca.eandb.util.args.IntegerFieldOption;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.io.DocumentOutputStream;
import ca.eandb.util.progress.ProgressPanel;

/**
 * @author Brad Kimmel
 *
 */
public final class MainWindow extends JFrame {

	private static final boolean SYSTEM_TRAY_SUPPORTED = Double.parseDouble(System.getProperty("java.specification.version")) >= 1.6;

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(MainWindow.class);  //  @jve:decl-index=0:
	private static final Preferences pref = Preferences.userNodeForPackage(MainWindow.class);
	private static final int RECONNECT_TIMEOUT = 60;
	private JPanel jContentPane = null;
	private JSplitPane jSplitPane = null;
	private ProgressPanel progressPanel = null;
	private JEditorPane consolePane = null;
	private JScrollPane consoleScrollPane = null;
	private ConnectionDialog connectionDialog = null;
	private JLabel statusLabel = null;
	private ThreadServiceWorker worker = null;
	private Thread workerThread = null;
	private final Options options;
	private final PowerCourtesyMonitor powerMonitor = CourtesyMonitorFactory.INSTANCE
			.createPowerCourtesyMonitor();

	/**
	 * Container class for command line options.
	 * @author Brad Kimmel
	 */
	public static final class Options {

		/** Number of CPUs to use to process worker threads. */
		public int numberOfCpus = -1;

		/** A value indicating if the program is being run on startup. */
		public boolean startup = false;

	};

	/**
	 * This method initializes jSplitPane
	 *
	 * @return javax.swing.JSplitPane
	 */
	private JSplitPane getJSplitPane() {
		if (jSplitPane == null) {
			jSplitPane = new JSplitPane();
			jSplitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
			jSplitPane.setDividerLocation(100);
			jSplitPane.setOneTouchExpandable(true);
			jSplitPane.setTopComponent(getProgressPanel());
			jSplitPane.setBottomComponent(getConsoleScrollPane());
			jSplitPane.setResizeWeight(1.0D);
		}
		return jSplitPane;
	}

	/**
	 * This method initializes progressPanel
	 *
	 * @return ca.eandb.util.progress.ProgressPanel
	 */
	private ProgressPanel getProgressPanel() {
		if (progressPanel == null) {
			progressPanel = new ProgressPanel();
		}
		return progressPanel;
	}

	/**
	 * This method initializes consolePane
	 *
	 * @return javax.swing.JEditorPane
	 */
	private JEditorPane getConsolePane() {
		if (consolePane == null) {
			consolePane = new JEditorPane();
			consolePane.setEditable(false);
			consolePane.setContentType("text/rtf");
		}
		return consolePane;
	}

	/**
	 * This method initializes consoleScrollPane
	 *
	 * @return javax.swing.JScrollPane
	 */
	private JScrollPane getConsoleScrollPane() {
		if (consoleScrollPane == null) {
			consoleScrollPane = new JScrollPane();
			consoleScrollPane.setViewportView(getConsolePane());
		}
		return consoleScrollPane;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ArgumentProcessor<Options> argProcessor = new ArgumentProcessor<Options>();
		argProcessor.addOption("ncpus", 'n', new IntegerFieldOption<Options>("numberOfCpus"));
		argProcessor.addOption("startup", 'S', new BooleanFieldOption<Options>("startup"));

		final Options options = new Options();
		argProcessor.process(args, options);

		if (options.startup && !pref.getBoolean("runOnStartup", true)) {
			return;
		}

		JdcpUtil.initialize();

		ConsoleAppender appender = new ConsoleAppender();
		appender.setLayout(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
		appender.setTarget(ConsoleAppender.SYSTEM_ERR);
		appender.setFollow(true);
		appender.activateOptions();
		BasicConfigurator.configure(appender);

		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				MainWindow thisClass = new MainWindow(options);
				if (!options.startup) {
					thisClass.setVisible(true);
				}
			}
		});
	}

	/**
	 * Creates a <code>MainWindow</code> using the specified
	 * <code>Options</code>.
	 * @param options The <code>Options</code> to use to configure the behavior
	 * 		of the program.
	 */
	public MainWindow(Options options) {
		super();
		this.options = options;
		initialize();
	}

	/**
	 * This is the default constructor
	 */
	public MainWindow() {
		this(new Options());
	}

	/**
	 * This method initializes this
	 *
	 * @return void
	 */
	private void initialize() {
		this.setSize(300, 200);
		this.setContentPane(getJContentPane());
		this.setTitle("JDCP Worker");
		this.addWindowListener(new WindowAdapter() {
			public void windowOpened(WindowEvent e) {
				connectConsole();
			}
		});

		JMenuBar menuBar = new JMenuBar();
		JMenu menu;
		JMenuItem item;

		menu = new JMenu("File");
		menu.setMnemonic('F');
		menuBar.add(menu);

		item = new JMenuItem("Change connection", 'c');
		item.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				changeConnection();
			}
		});
		menu.add(item);

		item = new JMenuItem("Preferences", 'p');
		item.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				openPreferences();
			}
		});
		menu.add(item);

		menu.addSeparator();

		item = new JMenuItem("Exit", 'x');
		item.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				exit();
			}
		});
		menu.add(item);

		this.setJMenuBar(menuBar);

		if (SYSTEM_TRAY_SUPPORTED) {
			initializeSystemTray();
		}

		startWorker();
	}

	/**
	 * Initializes the system tray icon.
	 *
	 * REQUIRES JAVA SE 6 OR HIGHER.
	 */
	private void initializeSystemTray() {
		if (SystemTray.isSupported()) {

			URL imageUrl = MainWindow.class.getResource("resources/jdcp-32.png");
			Image image = Toolkit.getDefaultToolkit().getImage(imageUrl);
			final TrayIcon icon = new TrayIcon(image, "JDCP Worker");
			icon.setImageAutoSize(true);

			PopupMenu popup = new PopupMenu();
			MenuItem popupItem = new MenuItem("Open JDCP Worker");
			popupItem.setShortcut(new MenuShortcut(KeyEvent.VK_O));
			popupItem.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					MainWindow.this.setVisible(true);
					MainWindow.this.setState(JFrame.NORMAL);
					MainWindow.this.toFront();
				}
			});
			popup.add(popupItem);

			popupItem = new MenuItem("Exit");
			popupItem.setShortcut(new MenuShortcut(KeyEvent.VK_X));
			popupItem.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					exit();
				}
			});
			popup.add(popupItem);

			icon.setPopupMenu(popup);
			icon.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					MainWindow.this.setVisible(true);
					MainWindow.this.setState(JFrame.NORMAL);
					MainWindow.this.toFront();
				}
			});

			setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
			addWindowListener(new WindowAdapter() {
				private boolean first = true;
				public void windowClosing(WindowEvent e) {
					MainWindow.this.setVisible(false);
					if (first) {
						icon.displayMessage(
								"JDCP Worker",
								"JDCP Worker is still running.  To exit, right click this icon and click 'exit'.",
								MessageType.INFO);
						first = false;
					}
				}
			});

			try {
				SystemTray.getSystemTray().add(icon);
			} catch (AWTException e) {
				logger.error("Could not add system tray icon.", e);
			}

		}
	}

	private void openPreferences() {
		final PreferencesDialog dialog = new PreferencesDialog(this);
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				dialog.setPowerSettingsEnabled(powerMonitor != null);
				dialog.setVisible(true);
				if (!dialog.isCancelled()) {
					onPreferencesChanged();
				}
			}
		});
	}

	private void onPreferencesChanged() {
		int maxCpus = pref.getInt("maxCpus", 0);
		int availableCpus = Runtime.getRuntime().availableProcessors();
		if (maxCpus <= 0 || maxCpus > availableCpus) {
			maxCpus = availableCpus;
		}
		worker.setMaxWorkers(maxCpus);

		if (powerMonitor != null) {
			boolean requireAC = pref.getBoolean("requireAC", false);
			int minBattLife = pref.getInt("minBattLife", 0);
			int minBattLifeWhileChg = pref.getInt("minBattLifeWhileChg", 0);
			minBattLife = Math.min(Math.max(minBattLife, 0), 100);
			minBattLifeWhileChg = Math.min(Math.max(minBattLifeWhileChg, 0), 100);
			powerMonitor.setRequireAC(requireAC);
			powerMonitor.setMinBatteryLifePercent(minBattLife);
			powerMonitor.setMinBatteryLifePercentWhileCharging(minBattLifeWhileChg);
			powerMonitor.update();
		}
	}

	private void changeConnection() {
		startWorker();
	}

	private void exit() {
		getConnectionDialog().dispose();
		System.exit(0);
	}

	private JobService reconnect() {
		return connect(RECONNECT_TIMEOUT);
	}

	private JobService connect() {
		return connect(0);
	}

	private JobService connect(int timeout) {
		final ConnectionDialog dialog = getConnectionDialog();
		JobService service = null;
		boolean showMessages = true;
		do {
			if (MainWindow.this.isVisible()) {
				dialog.setTimeout(timeout);
				try {
					SwingUtilities.invokeAndWait(new Runnable() {
						public void run() {
							dialog.setVisible(true);
						}
					});
					if (dialog.isCancelled()) {
						break;
					}
					showMessages = !dialog.isTimedOut();
				} catch (InterruptedException e) {
					break;
				} catch (InvocationTargetException e) {
					logger.error("Exception raised while displaying connection dialog.", e);
					break;
				}
			} else {
				final Thread thisThread = Thread.currentThread();
				WindowAdapter l = new WindowAdapter() {
					public void windowActivated(WindowEvent e) {
						thisThread.interrupt();
					}
				};
				MainWindow.this.addWindowListener(l);
				try {
					Calendar endTimeout = Calendar.getInstance();
					endTimeout.add(Calendar.SECOND, timeout);
					while (true) {
						Calendar now = Calendar.getInstance();
						if (!now.before(endTimeout)) {
							break;
						}
						long millis = endTimeout.getTimeInMillis()
								- now.getTimeInMillis();
						assert(millis > 0);
						try {
							Thread.sleep(millis);
						} catch (InterruptedException e) {
							if (MainWindow.this.isVisible()) {
								timeout = 0;
								break;
							}
						}
					}
					if (timeout == 0) {
						continue;
					}
				} finally {
					MainWindow.this.removeWindowListener(l);
				}
				showMessages = false;
			}
			service = connect(dialog.getHost(), dialog.getUser(), dialog.getPassword(), showMessages);
			if (!dialog.isTimedOut()) {
				timeout = 0;
			}
		} while (service == null);
		return service;
	}

	private JobService connect(String host, String user, String password, boolean showMessageDialog) {
		try {
			Registry registry = LocateRegistry.getRegistry(host, JdcpUtil.DEFAULT_PORT);
			AuthenticationService authService = (AuthenticationService) registry.lookup("AuthenticationService");
			return authService.authenticate(user, password, JdcpUtil.PROTOCOL_VERSION_ID);
		} catch (LoginException e) {
			logger.error("Authentication failed.", e);
			JOptionPane.showMessageDialog(this, "Authentication failed.  Please check your user name and password.", "Connection Failed", JOptionPane.WARNING_MESSAGE);
		} catch (RemoteException e) {
			logger.error("Could not connect to remote host.", e);
			if (showMessageDialog) {
				JOptionPane.showMessageDialog(this, "Could not connect to remote host.", "Connection Failed", JOptionPane.WARNING_MESSAGE);
			}
		} catch (NotBoundException e) {
			logger.error("Could not find AuthenticationService at remote host.", e);
			if (showMessageDialog) {
				JOptionPane.showMessageDialog(this, "Could find JDCP Server at remote host.", "Connection Failed", JOptionPane.WARNING_MESSAGE);
			}
		} catch (ProtocolVersionException e) {
			logger.error("Client is incompatible with the remote server.", e);
			if (showMessageDialog) {
				JOptionPane.showMessageDialog(this, "This client is incompatible with the remote server.  Please update the client and try again.", "Incompatible Client", JOptionPane.WARNING_MESSAGE);
			}
		}
		return null;
	}

	/**
	 * Start the worker thread.
	 */
	private void startWorker() {
		JobServiceFactory serviceFactory = new JobServiceFactory() {

			private boolean first = true;

			public JobService connect() {
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						setStatus("Connecting...");
					}
				});
				JobService service = first ? MainWindow.this.connect()
						: reconnect();
				if (service == null) {
					setVisible(false);
					throw new RuntimeException("Unable to connect");
				}
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						setStatus("Connected");
					}
				});
				first = false;
				return service;
			}
//
//			public JobService connect() {
//				try {
//					SwingUtilities.invokeAndWait(task);
//				} catch (Exception e) {
//					logger.warn("Exception thrown trying to reconnect", e);
//					throw new RuntimeException(e);
//				}
//				if (task.service == null) {
//					throw new RuntimeException("No service.");
//				}
//				return task.service;
//			}

		};

		int availableCpus = Runtime.getRuntime().availableProcessors();
		if (options.numberOfCpus < 0) {
			options.numberOfCpus = pref.getInt("maxCpus", 0);
		}
		if (options.numberOfCpus <= 0 || options.numberOfCpus > availableCpus) {
			options.numberOfCpus = availableCpus;
		}

		if (worker != null) {
			setStatus("Shutting down worker...");
			worker.shutdown();
			try {
				workerThread.join();
			} catch (InterruptedException e) {
			}
			progressPanel.clear();
		}

		setStatus("Starting worker...");

		CourtesyMonitor courtesyMonitor = (powerMonitor != null) ? powerMonitor
				: new UnconditionalCourtesyMonitor();
		ThreadFactory threadFactory = new BackgroundThreadFactory();
		worker = new ThreadServiceWorker(serviceFactory, threadFactory,
				getProgressPanel(), courtesyMonitor);
		worker.setMaxWorkers(options.numberOfCpus);

		boolean cacheClassDefinitions = pref.getBoolean("cacheClassDefinitions", true);
		if (cacheClassDefinitions) {
			setStatus("Preparing data source...");
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
		}

		onPreferencesChanged();

		workerThread = new Thread(worker);
		workerThread.start();

	}

	private void setStatus(final String status) {
		statusLabel.setText(" " + status);
	}

	/**
	 * This method initializes jContentPane
	 *
	 * @return javax.swing.JPanel
	 */
	private JPanel getJContentPane() {
		if (jContentPane == null) {
			statusLabel = new JLabel();
			statusLabel.setText(" ");
			jContentPane = new JPanel();
			jContentPane.setLayout(new BorderLayout());
			jContentPane.add(getJSplitPane(), BorderLayout.CENTER);
			jContentPane.add(statusLabel, BorderLayout.SOUTH);
		}
		return jContentPane;
	}

	private ConnectionDialog getConnectionDialog() {
		if (connectionDialog == null) {
			connectionDialog = new ConnectionDialog(this);
		}
		return connectionDialog;
	}

	public void connectConsole() {
		Document document = getConsolePane().getDocument();

		System.setOut(new PrintStream(new DocumentOutputStream(document, SimpleAttributeSet.EMPTY)));

		MutableAttributeSet attributes = new SimpleAttributeSet();
		StyleConstants.setForeground(attributes, Color.RED);
		System.setErr(new PrintStream(new DocumentOutputStream(document, attributes)));
	}

}
