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

package ca.eandb.jdcp.worker.policy;

import java.util.ArrayList;
import java.util.List;

/**
 * A <code>CourtesyMonitor</code> that is composes a collection of other
 * <code>CourtesyMonitor</code>s.  If any of the child
 * <code>CourtesyMonitor</code>s does not allow tasks to run, then this
 * <code>CompositeCourtesyMonitor</code> will not allow tasks to run.
 * @author Brad Kimmel
 */
public final class CompositeCourtesyMonitor implements CourtesyMonitor {

  /** The <code>List</code> of <code>CourtesyMonitor</code>s. */
  private final List<CourtesyMonitor> monitors = new ArrayList<CourtesyMonitor>();

  /**
   * Adds a <code>CourtesyMonitor</code> to this
   * <code>CompositeCourtesyMonitor</code>.
   * @param monitor The <code>CourtesyMonitor</code> to add.
   * @return This <code>CompositeCourtesyMonitor</code> (for method
   *     chaining).
   */
  public CompositeCourtesyMonitor add(CourtesyMonitor monitor) {
    monitors.add(monitor);
    return this;
  }

  @Override
  public boolean allowTasksToRun() {
    for (CourtesyMonitor monitor : monitors) {
      if (!monitor.allowTasksToRun()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void waitFor() throws InterruptedException {
    while (!allowTasksToRun()) {
      for (CourtesyMonitor monitor : monitors) {
        monitor.waitFor();
      }
    }
  }

}
