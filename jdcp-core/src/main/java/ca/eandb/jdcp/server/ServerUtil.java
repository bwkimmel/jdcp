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

package ca.eandb.jdcp.server;

import ca.eandb.jdcp.job.HostService;

/**
 * Provides access to information about the currently running job server.
 * @author Brad Kimmel
 */
public final class ServerUtil {

  /** The active <code>HostService</code>s. */
  private static final ThreadLocal<HostService> services = new ThreadLocal<HostService>();

  /**
   * Gets the active <code>HostService</code> for this thread.
   * @return The active <code>HostService</code> for this thread.
   */
  public static HostService getHostService() {
    return services.get();
  }

  /**
   * Sets the active <code>HostService</code> for this thread.
   * @param service The <code>HostService</code> to use for this thread.
   */
  /* package */ static void setHostService(HostService service) {
    services.set(service);
  }

  /**
   * Removes the active <code>HostService</code> for this thread.
   */
  /* package */ static void clearHostService() {
    services.set(null);
  }

  /** This constructor is private to prevent instances from being created. */
  private ServerUtil() {}

}
