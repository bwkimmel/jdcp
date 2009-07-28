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

package ca.eandb.jdcp.worker.policy.win32;

import java.awt.Window;

import javax.swing.JWindow;

import ca.eandb.jdcp.worker.policy.AsyncCourtesyMonitor;
import ca.eandb.jdcp.worker.policy.win32.Kernel32.SYSTEM_POWER_STATUS;
import ca.eandb.jdcp.worker.policy.win32.User32.WindowProc;
import ca.eandb.jdcp.worker.policy.win32.W32API.HWND;
import ca.eandb.jdcp.worker.policy.win32.W32API.LPARAM;
import ca.eandb.jdcp.worker.policy.win32.W32API.LRESULT;
import ca.eandb.jdcp.worker.policy.win32.W32API.WPARAM;
import ca.eandb.util.UnexpectedException;

import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * A <code>CourtesyMonitor</code> that monitors the power status of this
 * machine.
 * @author Brad Kimmel
 */
public final class Win32PowerCourtesyMonitor extends AsyncCourtesyMonitor {

	/**
	 * A value indicating whether tasks should run only if A/C power is
	 * connected.
	 */
	private boolean requireAC = true;

	/**
	 * This value sets the battery life percentage below which tasks will be
	 * suspended.  If {@link #requireAC} is set, this value has no effect.
	 */
	private byte minBatteryLifePercent = 0;

	/**
	 * This value sets the battery life percentage below which tasks will be
	 * suspended while the battery is charging.
	 */
	private byte minBatteryLifePercentWhileCharging = 0;

	/** Receives WM_POWERBROADCAST messages from Windows. */
	@SuppressWarnings("unused")
	private final PowerBroadcastMonitor monitor;

	/**
	 * A <code>WindowProc</code> for receiving WM_POWERBROADCAST messages
	 * from Windows.
	 */
	private final class PowerBroadcastMonitor implements WindowProc {

		/** A dummy <code>Window</code> to receive messages. */
		private final Window msgWindow = new JWindow();

		/** The original <code>WNDPROC</code> of the dummy window. */
		private final int prevWndProc;

		/**
		 * Creates a <code>PowerBroadcastMonitor</code>.
		 */
		public PowerBroadcastMonitor() {
			// The window needs to be made visible once to set its WindowProc.
			msgWindow.setVisible(true);
			msgWindow.setVisible(false);

			// Get a handle to the window.
			HWND hwnd = new HWND();
			hwnd.setPointer(Native.getWindowPointer(msgWindow));

			// Set the WindowProc so that this instance receives window
			// messages.
			prevWndProc = User32.INSTANCE.SetWindowLong(hwnd,
					User32.GWL_WNDPROC, this);
		}

		/**
		 * Handles window messages.
		 */
		public LRESULT callback(HWND hWnd, int uMsg, WPARAM wParam,
				LPARAM lParam) {
			// If the message is a WM_POWERBROADCAST, then update the
			// CourtesyMonitor.
			if (uMsg == User32.WM_POWERBROADCAST
					&& wParam.intValue() == User32.PBT_APMPOWERSTATUSCHANGE) {
				poll();
			}

			// Delegate other messages to the original WindowProc.
			return User32.INSTANCE.CallWindowProc(prevWndProc, hWnd, uMsg,
					wParam, lParam);
		}
	};

	/**
	 * Creates a <code>Win32PowerCourtesyMonitor</code>.
	 */
	public Win32PowerCourtesyMonitor() {
		if (!Platform.isWindows()) {
			throw new UnexpectedException("This class requires Windows");
		}
		monitor = new PowerBroadcastMonitor();
		poll();
	}

	/**
	 * @return the requireAC
	 */
	public synchronized final boolean isRequireAC() {
		return requireAC;
	}

	/**
	 * @param requireAC the requireAC to set
	 */
	public synchronized final void setRequireAC(boolean requireAC) {
		this.requireAC = requireAC;
	}

	/**
	 * @return the minBatteryLifePercent
	 */
	public synchronized final byte getMinBatteryLifePercent() {
		return minBatteryLifePercent;
	}

	/**
	 * @param minBatteryLifePercent the minBatteryLifePercent to set
	 */
	public synchronized final void setMinBatteryLifePercent(
			byte minBatteryLifePercent) {
		this.minBatteryLifePercent = minBatteryLifePercent;
	}

	/**
	 * @return the minBatteryLifePercentWhileCharging
	 */
	public synchronized final byte getMinBatteryLifePercentWhileCharging() {
		return minBatteryLifePercentWhileCharging;
	}

	/**
	 * @param minBatteryLifePercentWhileCharging the minBatteryLifePercentWhileCharging to set
	 */
	public synchronized final void setMinBatteryLifePercentWhileCharging(
			byte minBatteryLifePercentWhileCharging) {
		this.minBatteryLifePercentWhileCharging = minBatteryLifePercentWhileCharging;
	}

	/**
	 * Updates the state of this <code>CourtesyMonitor</code>.
	 */
	private synchronized void poll() {
		SYSTEM_POWER_STATUS status = new SYSTEM_POWER_STATUS();
		Kernel32.INSTANCE.GetSystemPowerStatus(status);

		switch (status.ACLineStatus) {
		case 0: // battery
			allow(!requireAC
					&& (status.BatteryLifePercent < 0 || status.BatteryLifePercent >= minBatteryLifePercent));
			break;

		case 1:	// A/C
		default:
			allow((status.BatteryFlag & 0x8) == 0 // not charging
					|| (status.BatteryLifePercent < 0 || status.BatteryLifePercent >= minBatteryLifePercentWhileCharging));
			break;

		}
	}

}
