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

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

/**
 * A <code>JDialog</code> for prompting the user for connection information.
 * @author Brad Kimmel
 */
public class ConnectionDialog extends JDialog {

	/**
	 * Serialization version ID.
	 */
	private static final long serialVersionUID = -5563554710634496583L;

	private JPanel jContentPane = null;
	private JPanel mainPanel = null;
	private JPanel buttonPanel = null;
	private JButton okButton = null;
	private JButton cancelButton = null;
	private JTextField hostField = null;
	private JTextField userField = null;
	private JPasswordField passwordField = null;
	private boolean cancelled = false;

	/**
	 * @param owner
	 */
	public ConnectionDialog(Frame owner) {
		super(owner);
		initialize();
	}

	/**
	 * This method initializes this
	 *
	 * @return void
	 */
	private void initialize() {
		this.setSize(300, 200);
		this.setTitle("Connect");
		this.setContentPane(getJContentPane());
		this.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
		this.setModalityType(ModalityType.APPLICATION_MODAL);
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
			jContentPane.add(getMainPanel(), BorderLayout.CENTER);
			jContentPane.add(getButtonPanel(), BorderLayout.SOUTH);
		}
		return jContentPane;
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
			c.weightx = 0.0D;
			mainPanel.add(new JLabel("Host"), c);

			c.gridx = 1;
			c.weightx = 1.0D;
			mainPanel.add(getHostField(), c);

			c.gridx = 0;
			c.gridy = 1;
			c.weightx = 0.0D;
			mainPanel.add(new JLabel("User"), c);

			c.gridx = 1;
			c.weightx = 1.0D;
			mainPanel.add(getUserField(), c);

			c.gridx = 0;
			c.gridy = 2;
			c.weightx = 0.0D;
			mainPanel.add(new JLabel("Password"), c);

			c.gridx = 1;
			c.weightx = 1.0D;
			mainPanel.add(getPasswordField(), c);

			c.gridx = 0;
			c.gridy = 3;
			c.weightx = 1.0D;
			c.weighty = 1.0D;
			c.gridwidth = 2;
			mainPanel.add(new JPanel(), c);
		}
		return mainPanel;
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
	 * This method initializes hostField
	 *
	 * @return javax.swing.JTextField
	 */
	private JTextField getHostField() {
		if (hostField == null) {
			hostField = new JTextField();
			hostField.setText("localhost");
		}
		return hostField;
	}

	/**
	 * This method initializes userField
	 *
	 * @return javax.swing.JTextField
	 */
	private JTextField getUserField() {
		if (userField == null) {
			userField = new JTextField();
			userField.setText("guest");
		}
		return userField;
	}

	/**
	 * This method initializes hostField
	 *
	 * @return javax.swing.JPasswordField
	 */
	private JPasswordField getPasswordField() {
		if (passwordField == null) {
			passwordField = new JPasswordField();
		}
		return passwordField;
	}

	/**
	 * Gets a value indicating whether the user clicked the "Cancel" button.
	 * @return A value indicating whether the user clicked the "Cancel" button.
	 */
	public boolean isCancelled() {
		return cancelled;
	}

	/**
	 * Gets the host name entered by the user.
	 * @return The host name.
	 */
	public String getHost() {
		return getHostField().getText();
	}

	/**
	 * Gets the user name entered by the user.
	 * @return The user name.
	 */
	public String getUser() {
		return getUserField().getText();
	}

	/**
	 * Gets the password entered by the user.
	 * @return The password.
	 */
	public String getPassword() {
		return new String(getPasswordField().getPassword());
	}

}
