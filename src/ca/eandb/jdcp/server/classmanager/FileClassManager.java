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

import java.io.File;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.eandb.util.UnexpectedException;
import ca.eandb.util.io.FileUtil;

/**
 * A <code>ParentClassManager</code> that stores class definitions in a
 * directory tree rooted at a provided location.
 * @author Brad Kimmel
 */
public final class FileClassManager extends AbstractClassManager implements
		ParentClassManager {

	/** The extension to give to class definition files. */
	private static final String CLASS_EXTENSION = ".class";

	/** The extension to give to class digest files. */
	private static final String DIGEST_EXTENSION = ".md5";

	/** The digest algorithm to use. */
	private static final String DIGEST_ALGORITHM = "MD5";

	/** The directory in which to store the current class definitions. */
	private final File currentDirectory;

	/**
	 * The directory in which to store class definitions for which there are
	 * still existing child <code>ClassManager</code>s referencing those
	 * classes, but for which the definition of the class was overwritten
	 * after the child <code>ClassManager</code> was created.
	 */
	private final File deprecatedDirectory;

	/**
	 * The directory in which to store class definitions that apply only to
	 * particular child <code>ClassManager</code>s.
	 */
	private final File childrenDirectory;

	/** The index of the next child <code>ClassManager</code>. */
	private int nextChildIndex = 0;

	/**
	 * A map keyed on class names.  Each class name is associated with a list
	 * indicating the values of {@link #nextChildIndex} in effect at the points
	 * when that class was redefined.  This allows child
	 * <code>ClassManager</code>s to find old class definitions (specifically,
	 * the one in effect when that child was created).
	 */
	private final Map<String, List<Integer>> deprecationMap = new HashMap<String, List<Integer>>();

	/** A list of references to the child <code>ClassManager</code>s. */
	private final List<Reference<ChildClassManager>> activeChildren = new ArrayList<Reference<ChildClassManager>>();

	/**
	 * A list of indices associated with released
	 * <code>ChildClassManager</code>s, but whose corresponding snapshot
	 * directories (under {@link #deprecatedDirectory}) have yet to be removed.
	 * Snapshot directories may only be removed when all children with smaller
	 * indices have been released.
	 */
	private final List<Integer> deprecationPendingList = new ArrayList<Integer>();

	/**
	 * Creates a new <code>FileClassManager</code>.
	 * @param rootDirectory The working directory.
	 * @throws IllegalArgumentException If <code>rootDirectory</code> does not
	 * 		refer to a directory.
	 */
	public FileClassManager(String rootDirectory) throws IllegalArgumentException {
		this(new File(rootDirectory));
	}

	/**
	 * Creates a new <code>FileClassManager</code>.
	 * @param rootDirectory The working directory.
	 * @throws IllegalArgumentException If <code>rootDirectory</code> does not
	 * 		refer to a directory.
	 */
	public FileClassManager(File rootDirectory) throws IllegalArgumentException {
		if (!rootDirectory.isDirectory()) {
			throw new IllegalArgumentException("rootDirectory must be a directory");
		}
		this.currentDirectory = new File(rootDirectory, "current");
		this.deprecatedDirectory = new File(rootDirectory, "deprecated");
		this.childrenDirectory = new File(rootDirectory, "children");
		currentDirectory.mkdir();
		deprecatedDirectory.mkdir();
		childrenDirectory.mkdir();
		FileUtil.clearDirectory(deprecatedDirectory);
		FileUtil.clearDirectory(childrenDirectory);
	}

	/**
	 * Gets the relative path (without the extension) for files associated with
	 * the given fully qualified class name.
	 * @param className The fully qualified class name.
	 * @return The relative path to a file associated with the class.
	 */
	private String getBaseFileName(String className) {
		return className.replace('.', '/');
	}

	/**
	 * Writes a class definition.
	 * @param directory The directory under which to write the class
	 * 		definition.
	 * @param name The fully qualified name of the class.
	 * @param def A <code>ByteBuffer</code> containing the class definition.
	 */
	private void writeClass(File directory, String name, ByteBuffer def) {
		writeClass(directory, name, def, computeClassDigest(def));
	}

	/**
	 * Writes a class definition.
	 * @param directory The directory under which to write the class
	 * 		definition.
	 * @param name The fully qualified name of the class.
	 * @param def A <code>ByteBuffer</code> containing the class definition.
	 * @param digest The MD5 digest of the class definition.
	 */
	private void writeClass(File directory, String name, ByteBuffer def, byte[] digest) {
		String baseName = getBaseFileName(name);
		File classFile = new File(directory, baseName + CLASS_EXTENSION);
		File digestFile = new File(directory, baseName + DIGEST_EXTENSION);

		try {
			FileUtil.setFileContents(classFile, def, true);
			FileUtil.setFileContents(digestFile, digest, true);
		} catch (IOException e) {
			e.printStackTrace();
			classFile.delete();
			digestFile.delete();
		}
	}

	/**
	 * Moves a class definition from the specified directory tree to another
	 * specified directory tree.
	 * @param fromDirectory The root of the directory tree from which to move
	 * 		the class definition.
	 * @param name The fully qualified name of the class to move.
	 * @param toDirectory The root of the directory tree to move the class
	 * 		definition to.
	 */
	private void moveClass(File fromDirectory, String name, File toDirectory) {
		String baseName = getBaseFileName(name);
		File fromClassFile = new File(fromDirectory, baseName + CLASS_EXTENSION);
		File toClassFile = new File(toDirectory, baseName + CLASS_EXTENSION);
		File fromDigestFile = new File(fromDirectory, baseName + DIGEST_EXTENSION);
		File toDigestFile = new File(toDirectory, baseName + DIGEST_EXTENSION);
		File toClassDirectory = toClassFile.getParentFile();

		toClassDirectory.mkdirs();
		fromClassFile.renameTo(toClassFile);
		fromDigestFile.renameTo(toDigestFile);
	}

	/**
	 * Determines if the specified class exists in the specified directory
	 * tree.
	 * @param directory The root of the directory tree to examine.
	 * @param name The fully qualified name of the class.
	 * @return A value indicating if the class exists in the given directory
	 * 		tree.
	 */
	private boolean classExists(File directory, String name) {
		String baseName = getBaseFileName(name);
		File classFile = new File(directory, baseName + CLASS_EXTENSION);
		File digestFile = new File(directory, baseName + DIGEST_EXTENSION);

		return classFile.isFile() && digestFile.isFile();
	}

	/**
	 * Gets the contents of the specified file, or null if the file does not
	 * exist.
	 * @param file The <code>File</code> whose contents to obtain.
	 * @return The file contents, or null if the file does not exist.
	 */
	private byte[] getFileContents(File file) {
		if (file.exists()) {
			try {
				return FileUtil.getFileContents(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * Gets the MD5 digest of a class definition.
	 * @param directory The root of the directory tree containing the class.
	 * @param name The fully qualified name of the class.
	 * @return The MD5 digest of the class definition.
	 */
	private byte[] getClassDigest(File directory, String name) {
		String baseName = getBaseFileName(name);
		File digestFile = new File(directory, baseName + DIGEST_EXTENSION);
		return getFileContents(digestFile);
	}

	/**
	 * Gets the definition of a class.
	 * @param directory The root of the directory tree containing the class
	 * 		definition.
	 * @param name The fully qualified name of the class.
	 * @return A <code>ByteBuffer</code> containing the class definition.
	 */
	private ByteBuffer getClassDefinition(File directory, String name) {
		String baseName = getBaseFileName(name);
		File digestFile = new File(directory, baseName + CLASS_EXTENSION);
		return ByteBuffer.wrap(getFileContents(digestFile));
	}

	/**
	 * Computes the MD5 digest of the given class definition.
	 * @param def A <code>ByteBuffer</code> containing the class definition.
	 * @return The MD5 digest of the class definition.
	 */
	private byte[] computeClassDigest(ByteBuffer def) {
		try {
			MessageDigest alg = MessageDigest.getInstance(DIGEST_ALGORITHM);
			def.mark();
			alg.update(def);
			def.reset();
			return alg.digest();
		} catch (NoSuchAlgorithmException e) {
			throw new UnexpectedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ClassManager#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) {
		return getClassDigest(currentDirectory, name);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ClassManager#setClassDefinition(java.lang.String, java.nio.ByteBuffer)
	 */
	public void setClassDefinition(String name, ByteBuffer def) {
		byte[] digest = computeClassDigest(def);
		if (classExists(currentDirectory, name)) {
			byte[] oldDigest = getClassDigest(currentDirectory, name);
			if (Arrays.equals(digest, oldDigest)) {
				return;
			}
			if (nextChildIndex > 0) {
				File deprecatedDirectory = getDeprecatedDirectory(name, nextChildIndex);
				if (!classExists(deprecatedDirectory, name)) {
					moveClass(currentDirectory, name, deprecatedDirectory);
					List<Integer> deprecationList = deprecationMap.get(name);
					if (deprecationList == null) {
						deprecationList = new ArrayList<Integer>();
						deprecationMap.put(name, deprecationList);
					}
					deprecationList.add(nextChildIndex);
				}
			}
		}
		writeClass(currentDirectory, name, def, digest);
	}

	private File getDeprecatedDirectory(String name, int childIndex) {
		return new File(deprecatedDirectory, Integer.toString(childIndex));
	}

	/* (non-Javadoc)
	 * @see ca.eandb.util.classloader.ClassLoaderStrategy#getClassDefinition(java.lang.String)
	 */
	public ByteBuffer getClassDefinition(String name) {
		return getClassDefinition(currentDirectory, name);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ParentClassManager#createChildClassManager()
	 */
	public ClassManager createChildClassManager() {
		ChildClassManager child = new ChildClassManager();
		activeChildren.add(new WeakReference<ChildClassManager>(child));
		return child;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ParentClassManager#releaseChildClassManager(ca.eandb.jdcp.server.classmanager.ClassManager)
	 */
	public void releaseChildClassManager(ClassManager childClassManager) {
		ChildClassManager child = (ChildClassManager) childClassManager;
		if (child.getParent() != this) {
			throw new IllegalArgumentException("childClassManager is not the child of this ParentClassManager");
		}
		releaseChildClassManager(child);
	}

	/**
	 * Releases the resources associated with a child
	 * <code>ClassManager</code>.
	 * @param child The child <code>ClassManager</code> to release.
	 */
	private void releaseChildClassManager(ChildClassManager child) {
		child.release();
		for (int i = 0; i < activeChildren.size(); i++) {
			Reference<ChildClassManager> ref = activeChildren.get(i);
			ChildClassManager current = ref.get();
			if (current.childIndex == child.childIndex) {
				FileUtil.deleteRecursive(child.childDirectory);
				deprecationPendingList.add(child.childIndex);
				if (i == 0) {
					Collections.sort(deprecationPendingList);
					for (int pendingIndex : deprecationPendingList) {
						if (pendingIndex > current.childIndex) {
							break;
						}
						File pendingDirectory = new File(deprecatedDirectory, Integer.toString(pendingIndex + 1));
						FileUtil.deleteRecursive(pendingDirectory);
					}
				}
			}
		}
	}

	/**
	 * A child <code>ClassManager</code> of a <code>FileClassManager</code>.
	 * @author Brad Kimmel
	 */
	private final class ChildClassManager extends AbstractClassManager {

		/**
		 * The root of the directory tree in which class definitions specific
		 * to this <code>ChildClassManager</code> are stored.
		 */
		private final File childDirectory;

		/** The index associated with this child. */
		private final int childIndex;

		/**
		 * A value indicating whether this <code>ChildClassManager</code> has
		 * been released.
		 */
		private boolean released = false;

		/**
		 * Creates a new <code>ChildClassManager</code>.
		 */
		public ChildClassManager() {
			this.childIndex = nextChildIndex++;
			this.childDirectory = new File(childrenDirectory, Integer
					.toString(childIndex));
		}

		/**
		 * Ensures that this <code>ChildClassManager</code> has not been
		 * released.
		 * @throws IllegalStateException if this <code>ChildClassManager</code>
		 * 		has been released.
		 */
		private void check() {
			if (released) {
				throw new IllegalStateException("Attempt to use a released child ClassManager.");
			}
		}

		/**
		 * Gets the root of the directory tree in which the current definition
		 * of the specified class associated with this
		 * <code>ChildClassManager</code> is stored.
		 * @param name The fully qualified name of the class.
		 * @return The root of the directory in which to find the current
		 * 		definition of the class associated with this
		 * 		<code>ChildClassManager</code>.
		 */
		private File getClassDirectory(String name) {
			check();
			if (classExists(childDirectory, name)) {
				return childDirectory;
			}

			List<Integer> deprecationList = deprecationMap.get(name);
			if (deprecationList != null) {
				int index = Collections.binarySearch(deprecationList, childIndex);
				index = Math.abs(index + 1);

				if (index < deprecationList.size()) {
					int deprecationIndex = deprecationList.get(index);
					File deprecatedDirectory = FileClassManager.this.getDeprecatedDirectory(name, deprecationIndex);
					if (!classExists(deprecatedDirectory, name)) {
						throw new UnexpectedException("Deprecated class missing.");
					}
					return deprecatedDirectory;
				}
			}

			return currentDirectory;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.server.classmanager.ClassManager#getClassDigest(java.lang.String)
		 */
		public byte[] getClassDigest(String name) {
			File directory = getClassDirectory(name);
			return FileClassManager.this.getClassDigest(directory, name);
		}

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.server.classmanager.ClassManager#setClassDefinition(java.lang.String, java.nio.ByteBuffer)
		 */
		public void setClassDefinition(String name, ByteBuffer def) {
			check();
			writeClass(childDirectory, name, def);
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.classloader.ClassLoaderStrategy#getClassDefinition(java.lang.String)
		 */
		public ByteBuffer getClassDefinition(String name) {
			File directory = getClassDirectory(name);
			return FileClassManager.this.getClassDefinition(directory, name);
		}

		/**
		 * Gets this <code>ChildClassManager</code>s parent
		 * <code>FileClassManager</code>.
		 * @return The <code>FileClassManager</code> that created this
		 * 		<code>ChildClassManager</code>.
		 */
		public FileClassManager getParent() {
			return FileClassManager.this;
		}

		/**
		 * Releases this <code>ChildClassManager</code>.
		 */
		private void release() {
			released = true;
		}

	}

}
