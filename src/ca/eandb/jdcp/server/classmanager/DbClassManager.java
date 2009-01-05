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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.eandb.util.UnexpectedException;
import ca.eandb.util.sql.DbUtil;

/**
 * @author Brad Kimmel
 *
 */
public final class DbClassManager extends AbstractClassManager implements ParentClassManager {

	private static final Logger logger = Logger.getLogger(DbClassManager.class);

	private static final Random rnd = new Random();

	private final DataSource ds;

	private final Map<Integer, DbChildClassManager> children = new HashMap<Integer, DbChildClassManager>();

	private int snapshotIndex = -1;

	public DbClassManager(DataSource ds) {
		this.ds = ds;
	}

	public void prepareDataSource() throws SQLException {
		Connection con = null;
		String sql;
		try {
			con = ds.getConnection();
			con.setAutoCommit(false);

			DatabaseMetaData meta = con.getMetaData();
			ResultSet rs = meta.getTables(null, null, "%Class%", new String[]{"TABLE"});
			int tableNameColumn = rs.findColumn("TABLE_NAME");
			int count = 0;
			while (rs.next()) {
				if (rs.getString(tableNameColumn) == "ParentClasses" ||
					rs.getString(tableNameColumn) == "ChildClasses" ||
					rs.getString(tableNameColumn) == "ChildClassManagers") {
					count++;
				}
			}

			if (count == 0) {
				String intType = DbUtil.getTypeName(Types.INTEGER, con);
				String blobType = DbUtil.getTypeName(Types.BLOB, con);
				String nameType = DbUtil.getTypeName(Types.VARCHAR, 1024, con);
				String md5Type = DbUtil.getTypeName(Types.BINARY, 16, con);

				sql =	"CREATE TABLE ParentClasses ( \n" +
						"  Name " + nameType + " NOT NULL, \n" +
						"  SnapshotIndex " + intType + " NOT NULL, \n" +
						"  Definition " + blobType + " NOT NULL, \n" +
						"  MD5 " + md5Type + " NOT NULL, \n" +
						"  PRIMARY KEY (Name, SnapshotIndex) \n" +
						")";
				DbUtil.update(ds, sql);

				sql =	"CREATE TABLE ChildClassManagers ( \n" +
						"  ChildID " + intType + " NOT NULL PRIMARY KEY, \n" +
						"  SnapshotIndex " + intType + " NOT NULL \n" +
						")";
				DbUtil.update(ds, sql);

				sql =	"CREATE TABLE ChildClasses ( \n" +
						"  ChildID " + intType + " NOT NULL, \n" +
						"  Name " + nameType + " NOT NULL, \n" +
						"  Definition " + blobType + " NOT NULL, \n" +
						"  MD5 " + md5Type + " NOT NULL, \n" +
						"  PRIMARY KEY (ChildID, Name), \n" +
						"  FOREIGN KEY (ChildID) REFERENCES ChildClassManagers(ChildID) \n" +
						")";
				DbUtil.update(ds, sql);
				con.commit();
			}

			con.setAutoCommit(true);
		} catch (SQLException e) {
			DbUtil.rollback(con);
			throw e;
		} finally {
			DbUtil.close(con);
		}
	}

	private int getSnapshotIndex(Connection con) throws SQLException {
		if (snapshotIndex < 0) {
			snapshotIndex = 1 + DbUtil.queryInt(con, -1,
					"SELECT MAX(SnapshotIndex) " +
					"FROM ChildClassManagers");
		}
		return snapshotIndex;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ParentClassManager#createChildClassManager()
	 */
	public ChildClassManager createChildClassManager() {
		Connection con = null;

		try {
			con = ds.getConnection();
			con.setAutoCommit(false);

			String sql =
					"SELECT COUNT(1) " +
					"FROM ChildClassManagers " +
					"WHERE ChildID = ?";

			int id;
			do {
				id = rnd.nextInt();
			} while (DbUtil.queryInt(con, 0, sql, id) > 0);

			int snapshot = getSnapshotIndex(con);
			DbUtil.update(con,
					"INSERT INTO ChildClassManagers " +
					"  (ChildID, SnapshotIndex) " +
					"VALUES (?, ?)",
					id, snapshot);

			con.commit();
			con.setAutoCommit(true);

			DbChildClassManager child = new DbChildClassManager(id);
			children.put(id, child);
			return child;

		} catch (SQLException e) {
			DbUtil.rollback(con);
			logger.error("Error communicating with class manager database.", e);
			throw new RuntimeException(e);
		} finally {
			DbUtil.close(con);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ParentClassManager#getChildClassManager(int)
	 */
	public ChildClassManager getChildClassManager(int id) {
		synchronized (children) {
			DbChildClassManager child = children.get(id);

			if (child == null) {
				try {
					String sql =
							"SELECT COUNT(1) " +
							"FROM ChildClassManagers " +
							"WHERE ChildID = ?";

					if (DbUtil.queryInt(ds, 0, sql, id) > 0) {
						child = new DbChildClassManager(id);
						children.put(id, child);
					}
				} catch (SQLException e) {
					logger.error("An error occurred while attempting to restore a child class manager from the database.", e);
				}
			}

			return child;
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ClassManager#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) {
		return getClassField(name, "MD5");
	}

	/* (non-Javadoc)
	 * @see ca.eandb.util.classloader.ClassLoaderStrategy#getClassDefinition(java.lang.String)
	 */
	public ByteBuffer getClassDefinition(String name) {
		byte[] def = getClassField(name, "Definition");
		return (def != null) ? ByteBuffer.wrap(def) : null;
	}

	private byte[] getClassField(String name, String field) {
		try {
			return DbUtil.queryBinary(ds, null,
					"SELECT " + field + " " +
					"FROM ParentClasses " +
					"WHERE Name = ? " +
					"ORDER BY SnapshotIndex DESC",
					name);
		} catch (SQLException e) {
			logger.error("Could not retrieve class definition from database.", e);
			throw new RuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.server.classmanager.ClassManager#setClassDefinition(java.lang.String, java.nio.ByteBuffer)
	 */
	public void setClassDefinition(String name, ByteBuffer def) {
		Connection con = null;
		try {
			con = ds.getConnection();
			con.setAutoCommit(false);

			int snapshot = getSnapshotIndex(con);

			byte[] bytes = new byte[def.remaining()];
			def.get(bytes);

			MessageDigest alg;
			try {
				alg = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				DbUtil.rollback(con);
				throw new UnexpectedException(e);
			}
			byte[] digest = alg.digest(bytes);

			String sql =
					"SELECT COUNT(1) " +
					"FROM ParentClasses " +
					"WHERE Name = ? " +
					"  AND SnapshotIndex = ?";
			if (DbUtil.queryInt(con, 0, sql, name, snapshot) > 0) {
				DbUtil.update(con,
						"UPDATE ParentClasses " +
						"SET " +
						"  Definition = ?, " +
						"  MD5 = ? " +
						"WHERE Name = ? " +
						"  AND SnapshotIndex = ?",
						bytes, digest, name, snapshot);
			} else {
				DbUtil.update(con,
						"INSERT INTO ParentClasses " +
						"  (SnapshotIndex, Name, Definition, MD5) " +
						"VALUES (?, ?, ?, ?)",
						snapshot, name, bytes, digest);
			}

			con.commit();
			con.setAutoCommit(true);
		} catch (SQLException e) {
			DbUtil.rollback(con);
			logger.error("Unable to persist class definition to database.", e);
			throw new RuntimeException(e);
		} finally {
			DbUtil.close(con);
		}
	}

	/**
	 * @author Brad Kimmel
	 *
	 */
	private final class DbChildClassManager extends AbstractClassManager
			implements ChildClassManager {

		private final int id;

		private boolean released = false;

		public DbChildClassManager(int id) {
			this.id = id;
		}

		private void check() {
			if (released) {
				throw new IllegalStateException();
			}
		}

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.server.classmanager.ChildClassManager#getChildId()
		 */
		public int getChildId() {
			check();
			return id;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.server.classmanager.ClassManager#setClassDefinition(java.lang.String, java.nio.ByteBuffer)
		 */
		public void setClassDefinition(String name, ByteBuffer def) {
			check();

			Connection con = null;
			try {
				con = ds.getConnection();
				con.setAutoCommit(false);

				byte[] bytes = new byte[def.remaining()];
				def.get(bytes);

				MessageDigest alg;
				try {
					alg = MessageDigest.getInstance("MD5");
				} catch (NoSuchAlgorithmException e) {
					DbUtil.rollback(con);
					throw new UnexpectedException(e);
				}
				byte[] digest = alg.digest(bytes);

				String sql =
						"SELECT COUNT(1) " +
						"FROM ChildClasses " +
						"WHERE ChildID = ? " +
						"  AND Name = ?";
				if (DbUtil.queryInt(con, 0, sql, id, name) > 0) {
					DbUtil.update(con,
							"UPDATE ChildClasses " +
							"SET " +
							"  Definition = ?, " +
							"  MD5 = ? " +
							"WHERE ChildID = ? " +
							"  AND Name = ?",
							bytes, digest, id, name);
				} else {
					DbUtil.update(con,
							"INSERT INTO ChildClasses " +
							"  (ChildID, Name, Definition, MD5) " +
							"VALUES (?, ?, ?, ?)",
							id, name, bytes, digest);
				}

				con.commit();
				con.setAutoCommit(true);
			} catch (SQLException e) {
				DbUtil.rollback(con);
				logger.error("Unable to persist class definition to database.", e);
				throw new RuntimeException(e);
			} finally {
				DbUtil.close(con);
			}
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.classloader.ClassLoaderStrategy#getClassDefinition(java.lang.String)
		 */
		public ByteBuffer getClassDefinition(String name) {
			byte[] def = getClassField(name, "Definition");
			return (def != null) ? ByteBuffer.wrap(def) : null;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.server.classmanager.ClassManager#getClassDigest(java.lang.String)
		 */
		public byte[] getClassDigest(String name) {
			return getClassField(name, "MD5");
		}

		private byte[] getClassField(String name, String field) {
			check();

			try {
				byte[] data = DbUtil.queryBinary(ds, null,
						"SELECT " + field + " " +
						"FROM ChildClasses " +
						"WHERE ChildID = ? " +
						"  AND Name = ?",
						id, name);
				if (data != null) {
					return data;
				}

				return DbUtil.queryBinary(ds, null,
						"SELECT " + field + " " +
						"FROM ChildClassManagers, ParentClasses " +
						"WHERE ChildClassManagers.ChildID = ? " +
						"  AND ParentClasses.SnapshotIndex <= ChildClassManagers.SnapshotIndex " +
						"  AND ParentClasses.Name = ? " +
						"ORDER BY ParentClasses.SnapshotIndex DESC",
						id, name);
			} catch (SQLException e) {
				logger.error("Could not retrieve class digest from database.", e);
				throw new RuntimeException(e);
			}
		}

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.server.classmanager.ChildClassManager#release()
		 */
		public void release() {
			Connection con = null;
			try {
				con = ds.getConnection();
				con.setAutoCommit(false);

				DbUtil.update(con,
						"DELETE FROM ChildClasses " +
						"WHERE ChildID = ?",
						id);

				DbUtil.update(con,
						"DELETE FROM ChildClassManagers " +
						"WHERE ChildID = ?",
						id);

				con.commit();
				con.setAutoCommit(true);
				released = true;
			} catch (SQLException e) {
				DbUtil.rollback(con);
				logger.error("Failed to remove child class manager from database.", e);
			} finally {
				DbUtil.close(con);
			}
		}

	}

}
