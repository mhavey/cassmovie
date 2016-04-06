package org.jude.bigdata.recroom.movies.etl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * Provides an interface to connect to cassandra db and insert/update/find
 * tables. This is quite a tactical class, providing JUST ENOUGH to support the
 * movie DB load.
 * 
 * @author user
 * 
 */
public class CassDBConnection {

	/**
	 * Inner class to maintain prepared statement definition. A prepared
	 * statement has a name, used to index it in our prepared statement cache.
	 * It also has a set of columns names that positionally match the
	 * substitutable params in the statement. It also can refer to a set of
	 * follow-on statements
	 * 
	 * @author Tibcouser
	 * 
	 */
	public static class PreparedStatementDefinition {
		String statementName;
		PreparedStatement statement;
		String columns[];
		String followonStatements[] = null;

		/**
		 * @param statementName
		 * @param statement
		 * @param columns
		 */
		public PreparedStatementDefinition(String statementName, PreparedStatement statement, String... columns) {
			this.statementName = statementName;
			this.statement = statement;
			this.columns = columns;
		}

		public PreparedStatementDefinition addFollowonUpdateStatements(String... foStatements) {
			this.followonStatements = foStatements;
			return this;
		}

		public PreparedStatement getStatement() {
			return this.statement;
		}

		public String[] getColumns() {
			return this.columns;
		}

		public String[] getFollowonStatements() {
			return this.followonStatements;
		}

		public String getStatementName() {
			return this.statementName;
		}
	}

	/**
	 * A callback class for async CQL executions.
	 * 
	 * @author Tibcouser
	 * 
	 */
	static class CassFutureCallback implements FutureCallback<ResultSet> {
		CassDBConnection cConnection = null;
		PreparedStatementDefinition statement;
		ImdbRecord record;
		ImdbIterator iterator;

		public CassFutureCallback(CassDBConnection cConnection, PreparedStatementDefinition statement,
				ImdbRecord record, ImdbIterator iterator) {
			this.cConnection = cConnection;
			this.statement = statement;
			this.record = record;
			this.iterator = iterator;
		}

		/**
		 * Notification of success on async CQL execution update
		 */
		public void onSuccess(ResultSet rs) {
			if (rs.wasApplied()) {
				this.iterator.logSuccess();
			} else {
				String explanation = "";
				Iterator<Row> rows = rs.all().iterator();
				rs.getColumnDefinitions().asList().get(0).getName();
				while (rows.hasNext()) {
					Row row = rows.next();
					explanation += " row: ";
					for (ColumnDefinitions.Definition def : row.getColumnDefinitions()) {
						explanation += def.getName() + "=" + row.getObject(def.getName());
					}
				}
				this.iterator
						.logFailure(
								new ETLException(ETLConstants.ERR_DUPE,
										"Update failed on statement *" + this.statement.getStatementName()
												+ "*  and record " + this.record + " explanation is " + explanation),
								this.record, true);
			}
		}

		/**
		 * Notification of success on async CQL execution.
		 */
		public void onFailure(Throwable t) {
			this.cConnection.onCQLResultFailure(this.statement.getStatementName(), this.record, this.iterator, t);
		}
	}

	public static final int MAX_PENDING = 2000;

	String nodes[] = null;
	String keyspace = null;
	Cluster cluster;
	Session session;
	Map<String, PreparedStatementDefinition> statements = new HashMap<String, PreparedStatementDefinition>();
	Map<String, UserType> udtDefs = new HashMap<String, UserType>();

	Logger logger = Logger.getLogger(CassDBConnection.class);

	/**
	 * Default constructor does nothing,
	 */
	public CassDBConnection() {

	}

	/**
	 * create a prepared statement having the given name, cql expression, and
	 * bound column names. If it already exists, just return it.
	 * 
	 * @param name
	 * @param cql
	 * @param columns
	 * @throws ETLException
	 */
	public PreparedStatementDefinition createStatement(String name, String cql, String... columns) throws ETLException {
		PreparedStatementDefinition def = this.statements.get(name);
		if (def != null) {
			throw new ETLException(ETLConstants.ERR_DUPE, "statement *" + name + "* duplicate");
		}
		PreparedStatement stmt = this.session.prepare(cql);
		def = new PreparedStatementDefinition(name, stmt, columns);
		this.statements.put(name, def);
		return def;
	}

	/**
	 * create UDT with given name
	 * 
	 * @param name
	 * @throws ETLException
	 */
	public void createUDT(String name) {
		if (this.udtDefs.containsKey(name)) {
			return;
		}
		UserType udt = this.session.getCluster().getMetadata().getKeyspace(this.keyspace).getUserType(name);
		this.udtDefs.put(name, udt);
	}

	/**
	 * obtain the statement to be used in a query or update
	 * 
	 * @param name
	 * @return
	 * @throws ETLException
	 */
	PreparedStatementDefinition getStatement(String name) throws ETLException {
		PreparedStatementDefinition stmt = this.statements.get(name);
		if (stmt == null) {
			throw new ETLException(ETLConstants.ERR_STATEMENT_NOT_FOUND, "Unable to find statement *" + name + "*");
		}
		return stmt;
	}

	/**
	 * obtain the UDT with the given name
	 * 
	 * @param name
	 * @return
	 * @throws ETLException
	 */
	UserType getUDT(String name) throws ETLException {
		UserType udt = this.udtDefs.get(name);
		if (udt == null) {
			throw new ETLException(ETLConstants.ERR_FIELD_NOT_FOUND, "Unable to UDT *" + name + "*");
		}
		return udt;
	}

	/**
	 * Gets configuration from specific properties
	 * 
	 * @param props
	 * @throws ETLException
	 */
	public void configure(ETLProperties props) throws ETLException {
		this.keyspace = props.getString(ETLConstants.PROP_CASS_KEYSPACE);
		this.nodes = props.getString(ETLConstants.PROP_CASS_NODES).split(",");
	}

	/**
	 * Connects to Cassandra
	 * 
	 * @param props
	 * @throws ETLException
	 */
	public void connect() throws ETLException {
		// Connect to a Cassandra cluster
		logger.debug("Attempting to connect to " + Arrays.toString(this.nodes) + " " + this.keyspace);
		this.cluster = Cluster.builder().addContactPoints(nodes).build();
		this.session = this.cluster.connect(this.keyspace);

		// Display data about the connection
		Metadata metadata = this.cluster.getMetadata();
		logger.info("Connected to cluster: " + metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			logger.info(
					"Datacenter " + host.getDatacenter() + " Host " + host.getAddress() + " Rack " + host.getRack());
		}
	}

	/**
	 * Disconnects from Cassandra
	 * 
	 * @throws ETLException
	 */
	public void disconnect() throws ETLException {

		if (this.cluster != null) {
			this.cluster.close();
		}
	}

	/**
	 * truncates the given table
	 * 
	 * @param tableName
	 * @throws ETLException
	 */
	public void truncate(String tableName) throws ETLException {
		ResultSet rs = this.session.execute("truncate " + tableName);
		if (rs.wasApplied()) {
			return;
		}
		throw new ETLException(ETLConstants.ERR_CASS, "Truncate failed. result set is " + rs.all());
	}

	/**
	 * Run async CQL (either an update or a sync existence-select followed by
	 * one or more updates) and report results back via callback.
	 * 
	 * @param statementName
	 * @param record
	 * @param iterator
	 */
	public void runAsyncCQL(String statementName, ImdbRecord record, ImdbIterator iterator) {
		runCQL(statementName, record, iterator, false);
	}

	/**
	 * Run the CQL having specified name using the specified record as input.
	 * 
	 * @param statementName
	 *            - Name of a previously defined prepared statement. It is
	 *            either a select or an insert/update. If it is a select, it's
	 *            job is to check existence of a record. If the record exists,
	 *            there will be one or more follow-on updates, which incorporate
	 *            fields returned in the query.
	 * @param record
	 *            - Input of the CQL.
	 * @param iterator
	 *            - ImdbIterator instance against which CQL result is logged.
	 * @return
	 * @throws ETLException
	 */
	void runCQL(String statementName, ImdbRecord record, ImdbIterator iterator, boolean fromCallback) {

		// 0. if there are a lot of pending requests, wait for a bit (async
		// only)
		if (!fromCallback && iterator.getNumPending() > MAX_PENDING) {
			logger.warn("A lot of pendings. Time to take a breather.");
			iterator.runTo(MAX_PENDING / 2);
			logger.warn("Breater is done");
		}

		logger.debug("Run CQL on " + statementName + " for record " + record);

		PreparedStatementDefinition stmt = null;
		iterator.addPending();

		try {
			// 1. find the statement
			stmt = getStatement(statementName);
			BoundStatement bstmt = stmt.getStatement().bind();

			// 2. put bound values in proper form
			String cols[] = stmt.getColumns();
			for (int i = 0; i < cols.length; i++) {
				String column = cols[i];
				Object nextVal = record.get(column);
				if (nextVal == null) {
					bstmt.setToNull(i);
				} else if (nextVal instanceof String) {
					bstmt.setString(i, (String) nextVal);
				} else if (nextVal instanceof Integer) {
					bstmt.setInt(i, (int) nextVal);
				} else if (nextVal instanceof Float) {
					bstmt.setFloat(i, (float) nextVal);
				} else if (nextVal instanceof Set) {
					bstmt.setSet(i, (Set<String>) nextVal);
				} else if (nextVal instanceof ImdbRecord) {
					UserType udt = getUDT(column);
					UDTValue uv = udt.newValue();
					// for simplicity, our model's UDTs only contain string
					// values,
					// so we'll create the UDT inline here now
					// in the general case we would handle UDT containing
					// anything,
					// maybe even another UDRJF
					ImdbRecord subdoc = (ImdbRecord) nextVal;
					Iterator<String> iterKeys = subdoc.keys().iterator();
					while (iterKeys.hasNext()) {
						String ukey = iterKeys.next();
						Object val = subdoc.get(ukey);
						if (val instanceof String) {
							uv.setString(ukey, (String) val);
						} else if (val instanceof Integer) {
							uv.setInt(ukey, (int) val);
						} else if (val instanceof Float) {
							uv.setFloat(ukey, (float) val);
						} else if (val instanceof Set) {
							uv.setSet(ukey, (Set<String>) val);
						} else {
							throw new ETLException(ETLConstants.ERR_CONVERSION, "Illegal UDT type *" + val
									+ "* of type " + val.getClass() + " in bound values " + Arrays.toString(cols));
						}
					}
					bstmt.setUDTValue(i, uv);
				} else {
					throw new ETLException(ETLConstants.ERR_CONVERSION,
							"Can't make sense of column " + i + " in bound values " + Arrays.toString(cols));
				}
			}

			// 3. If this is a query (with followup updates)
			// do the command synchronously.
			if (stmt.getFollowonStatements() != null && stmt.getFollowonStatements().length > 0) {

				ResultSet rs = this.session.execute(bstmt);

				// ok, it's a query and, in this class ALWAYS, an existence
				// check with follow-on updates
				List<Row> rows = rs.all();
				if (rows.size() > 1) {
					ETLException ex = new ETLException(ETLConstants.ERR_DUPE,
							"Existing check finds multiple rows on statement *" + statementName + "* with values "
									+ record + " result set is " + rows);
					iterator.logFailure(ex, record, true);
				} else if (rows.size() == 0) {
					ETLException ex = new ETLException(ETLConstants.ERR_RECORD_NOT_FOUND,
							"Existing check found no rows  on statement *" + statementName + "* with values " + record);
					iterator.logFailure(ex, record, true);
				} else {
					// we have the response; now we need to (a) Merge response
					// into
					// IMDB record, (b) run the updates
					Row row = rows.get(0);
					int numCols = rs.getColumnDefinitions().size();
					for (int i = 0; i < numCols; i++) {
						if (!row.isNull(i)) {
							Object o = row.getObject(i);
							if (o instanceof UDTValue) {
								// if the column is a UDT, convert to
								// ImdbRecord. As above,
								// assume it's all strings
								UDTValue udt = (UDTValue) o;
								ImdbRecord udtRec = new ImdbRecord(udt.getType().getTypeName());
								int numUDTCols = udt.getType().size();
								String fieldNames[] = new String[numUDTCols];
								udt.getType().getFieldNames().toArray(fieldNames);
								for (int j = 0; j < numUDTCols; j++) {
									if (udt.isNull(j)) {
										continue;
									}
									udtRec.append(fieldNames[j], udt.getObject(j));
								}
								record.getSubdoc(udt.getType().getTypeName()).merge(udtRec);
							} else {
								record.append(rs.getColumnDefinitions().getName(i), row.getObject(i));
							}
						}
					}

					// run the followups
					for (int i = 0; i < stmt.getFollowonStatements().length; i++) {
						runAsyncCQL(stmt.getFollowonStatements()[i], record, iterator);
					}
					iterator.logSuccess();
				}
			} else {
				// 4. run the update asynchronously
				ResultSetFuture future = this.session.executeAsync(bstmt);
				Futures.addCallback(future, new CassFutureCallback(this, stmt, record, iterator));
			}
		} catch (Throwable t) {
			onCQLResultFailure(statementName, record, iterator, t);
		}
	}

	/**
	 * Local method - handle failure result of CQL execution. Can be triggered
	 * from several places - foreground, callback, etc.
	 * 
	 * @param statement
	 * @param record
	 * @param iterator
	 * @param t
	 * @param async
	 */
	void onCQLResultFailure(String statementName, ImdbRecord record, ImdbIterator iterator, Throwable t) {
		t.printStackTrace();
		// these errors are bad enough for controller job to bail.
		if (t instanceof NoHostAvailableException) { // || t instanceof WriteTimeoutException) {
			System.exit(1);
		}
		ETLException ex = new ETLException(ETLConstants.ERR_CASS,
				"Got failure callback executing cql statement " + statementName + " on record " + record, t);
		iterator.logFailure(ex, record, true);
	}
}
