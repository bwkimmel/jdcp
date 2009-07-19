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
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.prefs.Preferences;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JPanel;

/**
 * @author Brad
 *
 */
public final class PreferencesDialog extends JDialog {

	private static final long serialVersionUID = 1L;
	private JPanel jContentPane = null;
	private JPanel buttonPanel = null;
	private JPanel mainPanel = null;
	private JCheckBox runOnStartupCheckBox = null;
	private JButton okButton;
	private JButton cancelButton;
	private boolean cancelled;

	/**
	 * @param owner
	 */
	public PreferencesDialog(Frame owner) {
		super(owner);
		initialize();
	}

	/* (non-Javadoc)
	 * @see java.awt.Dialog#setVisible(boolean)
	 */
	@Override
	public void setVisible(boolean b) {
		if (b) {
			readPreferences();
		}
		super.setVisible(b);
	}

	/**
	 * Determines if the user clicked the cancel button.
	 * @return A value indicating whether the user clicked the cancel button.
	 */
	public boolean isCancelled() {
		return cancelled;
	}

	/**
	 * Updates the form elements based on preferences.
	 */
	private void readPreferences() {
		Preferences pref = Preferences.userNodeForPackage(MainWindow.class);
		boolean runOnStartup = pref.getBoolean("runOnStartup", true);
		getRunOnStartupCheckBox().setSelected(runOnStartup);
	}

	/**
	 * Writes the form values to preferences.
	 */
	private void writePreferences() {
		Preferences pref = Preferences.userNodeForPackage(MainWindow.class);
		boolean runOnStartup = getRunOnStartupCheckBox().isSelected();
		pref.putBoolean("runOnStartup", runOnStartup);
	}

	/**
	 * This method initializes this
	 *
	 * @return void
	 */
	private void initialize() {
		this.setSize(300, 200);
		this.setTitle("Preferences");
		this.setContentPane(getJContentPane());
	}

	/**
	 * This method initializes jContentPane
	 *
	 * @return javax.swing.JPanel
	 */
	private JPanel getJContentPane() {
		if (jContentPane == null) {
			jContentPane = new JPanel();
			jContentPane.setLayout(new BorderLayout());
			jContentPane.add(getButtonPanel(), BorderLayout.SOUTH);
			jContentPane.add(getMainPanel(), BorderLayout.CENTER);
		}
		return jContentPane;
	}

	/**
	 * This method initializes buttonPanel
	 *
	 * @return javax.swing.JPanel
	 */
	private JPanel getButtonPanel() {
		if (buttonPanel == null) {
			buttonPanel = new JPanel();
			buttonPanel.setLayout(new GridBagLayout());

			GridBagConstraints c1 = new GridBagConstraints();
			c1.anchor = GridBagConstraints.EAST;
			c1.gridx = 0;
			c1.gridy = 0;
			c1.weightx = 1.0D;
			c1.insets = new Insets(5, 5, 5, 5);
			buttonPanel.add(getOkButton(), c1);

			GridBagConstraints c2 = new GridBagConstraints();
			c2.gridx = 1;
			c2.gridy = 0;
			c2.weightx = 0.0D;
			c2.insets = new Insets(5, 5, 5, 5);
			buttonPanel.add(getCancelButton(), c2);
		}
		return buttonPanel;
	}

	/**
	 * This method initializes okButton
	 *
	 * @return javax.swing.JButton
	 */
	private JButton getOkButton() {
		if (okButton == null) {
			okButton = new JButton();
			okButton.setText("OK");
			okButton.setMnemonic('O');
			okButton.addActionListener(new java.awt.event.ActionListener() {
				public void actionPerformed(java.awt.event.ActionEvent e) {
					cancelled = false;
					writePreferences();
					setVisible(false);
				}
			});
		}
		return okButton;
	}

	/**
	 * This method initializes cancelButton
	 *
	 * @return javax.swing.JButton
	 */
	private JButton getCancelButton() {
		if (cancelButton == null) {
			cancelButton = new JButton();
			cancelButton.setText("Cancel");
			cancelButton.setMnemonic('C');
			cancelButton.addActionListener(new java.awt.event.ActionListener() {
				public void actionPerformed(java.awt.event.ActionEvent e) {
					cancelled = true;
					setVisible(false);
				}
			});
		}
		return cancelButton;
	}

	/**
	 * This method initializes mainPanel
	 *
	 * @return javax.swing.JPanel
	 */
	private JPanel getMainPanel() {
		if (mainPanel == null) {
			mainPanel = new JPanel();
			mainPanel.setLayout(new GridBagLayout());

			GridBagConstraints c = new GridBagConstraints();
			c.insets = new Insets(5, 5, 5, 5);
			c.fill = GridBagConstraints.HORIZONTAL;

			c.gridx = 0;
			c.gridy = 0;
			c.gridwidth = 2;
			c.weightx = 1.0D;
			c.weighty = 0.0D;
			mainPanel.add(getRunOnStartupCheckBox(), c);

			c.gridx = 0;
			c.gridy = 1;
			c.gridwidth = 2;
			c.weightx = 1.0D;
			c.weighty = 1.0D;
			mainPanel.add(new JPanel(), c);
		}
		return mainPanel;
	}

	/**
	 * This method initializes runOnStartupCheckBox
	 *
	 * @return javax.swing.JCheckBox
	 */
	private JCheckBox getRunOnStartupCheckBox() {
		if (runOnStartupCheckBox == null) {
			runOnStartupCheckBox = new JCheckBox("Run on startup");
		}
		return runOnStartupCheckBox;
	}

}
