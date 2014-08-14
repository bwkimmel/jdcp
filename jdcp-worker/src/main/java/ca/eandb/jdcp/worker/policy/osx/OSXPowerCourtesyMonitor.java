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

package ca.eandb.jdcp.worker.policy.osx;

import com.sun.jna.Platform;

import ca.eandb.jdcp.worker.policy.AsyncCourtesyMonitor;
import ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor;
import ca.eandb.util.UnexpectedException;

/**
 * A <code>PowerCourtesyMonitor</code> for Mac OS X.  Not yet implemented.
 * @author Brad Kimmel
 */
public final class OSXPowerCourtesyMonitor extends AsyncCourtesyMonitor
		implements PowerCourtesyMonitor {

	/**
	 * A value indicating whether tasks should run only if A/C power is
	 * connected.
	 */
	private boolean requireAC = true;

	/**
	 * This value sets the battery life percentage below which tasks will be
	 * suspended.  If {@link #requireAC} is set, this value has no effect.
	 */
	private int minBatteryLifePercent = 0;

	/**
	 * This value sets the battery life percentage below which tasks will be
	 * suspended while the battery is charging.
	 */
	private int minBatteryLifePercentWhileCharging = 0;

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#isRequireAC()
	 */
	public final boolean isRequireAC() {
		return requireAC;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#setRequireAC(boolean)
	 */
	public final void setRequireAC(boolean requireAC) {
		this.requireAC = requireAC;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#getMinBatteryLifePercent()
	 */
	public final int getMinBatteryLifePercent() {
		return minBatteryLifePercent;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#setMinBatteryLifePercent(int)
	 */
	public final void setMinBatteryLifePercent(int minBatteryLifePercent) {
		this.minBatteryLifePercent = minBatteryLifePercent;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#getMinBatteryLifePercentWhileCharging()
	 */
	public final int getMinBatteryLifePercentWhileCharging() {
		return minBatteryLifePercentWhileCharging;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#setMinBatteryLifePercentWhileCharging(int)
	 */
	public final void setMinBatteryLifePercentWhileCharging(
			int minBatteryLifePercentWhileCharging) {
		this.minBatteryLifePercentWhileCharging = minBatteryLifePercentWhileCharging;
	}

	/**
	 * Creates a new <code>OSXPowerCourtesyMonitor</code>.
	 */
	public OSXPowerCourtesyMonitor() {
		if (!Platform.isMac()) {
			throw new UnexpectedException("This class requires Mac OS");
		}
		update();
		// TODO Start a thread to receive events when the power status changes.
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.PowerCourtesyMonitor#update()
	 */
	public void update() {
		// TODO Determine the power status and call allow() or disallow()
		// depending on that status.
		allow(true);
	}

}
