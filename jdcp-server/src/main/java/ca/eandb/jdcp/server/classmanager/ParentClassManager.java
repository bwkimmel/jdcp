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

package ca.eandb.jdcp.server.classmanager;

/**
 * A <code>ClassManager</code> that allows child <code>ClassManager</code>s to
 * be created.  Child <code>ClassManager</code>s are created as snapshots of
 * its parent.  If a class is redefined in the parent <code>ClassManager</code>,
 * the child will not be affected.  If a class is redefined in the child, the
 * parent will remain unaffected.
 * @author Brad Kimmel
 */
public interface ParentClassManager extends ClassManager {

  /**
   * Creates a new child <code>ClassManager</code>.
   * @return The new child <code>ClassManager</code>.
   */
  ChildClassManager createChildClassManager();

  /**
   * Retrieves an existing <code>ChildClassManager</code>.
   * @param id The identifier for the <code>ChildClassManager</code> to
   *     retrieve.
   * @return The <code>ChildClassManager</code> with the specified ID, or
   *     <code>null</code> if there is no such child.
   */
  ChildClassManager getChildClassManager(int id);

}
