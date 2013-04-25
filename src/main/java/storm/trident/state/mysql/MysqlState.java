package storm.trident.state.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class MysqlState<T> implements IBackingMap<T> {

	private Connection connection;
	private MysqlStateConfig config;
	private static final Logger logger = Logger.getLogger(MysqlState.class);

	MysqlState(final MysqlStateConfig config) {
		this.config = config;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(config.getUrl());
		} catch (final SQLException | ClassNotFoundException ex) {
			logger.error("Failed to establish DB connection", ex);
		}
	}

	/**
	 * factory method for the factory
	 * 
	 * @param config
	 * @return
	 */
	public static Factory newFactory(final MysqlStateConfig config) {
		return new Factory(config);
	}

	/**
	 * multiget implementation for mongodb
	 * 
	 */
	@Override
	@SuppressWarnings({"unchecked","rawtypes"})
	public List<T> multiGet(final List<List<Object>> keys) {
		if (keys.isEmpty()) {
			return new ArrayList<>();
		}
		// build a query using select key1, keys2, ..., val1, val2, ..., [txid], [prev_val1], ... FROM table WHERE (key1 = ? AND keys = ? ...) OR ...
		final StringBuilder queryBuilder = new StringBuilder().append("SELECT ")
			.append(buildColumns())
			.append(" FROM ")
			.append(config.getTable())
			.append(" WHERE ")
			.append(buildKeyQuery(keys.size()));
		final Map<List<Object>, List<Object>> queryResults = query(queryBuilder.toString(), keys);
		// build the value list by ordering based on the input keys and looking up the query results, transform to transactional and opaque values as needed
		return Lists.transform(keys, new Function<List<Object>, T>() {
			@Override
			public T apply(final List<Object> key) {
				final List<Object> values = queryResults.get(key);
				if (values == null) {
					return null;
				} else {
					switch (config.getType()) {
					case OPAQUE: // partition the values list into 3 values [current], [txid], [prev]
						return (T) new OpaqueValue((Long) values.get(config.getValueColumns().length), // txid
							values.subList(0, config.getValueColumns().length), // curr values
							values.subList(config.getValueColumns().length, values.size())); // prev values
					case TRANSACTIONAL:
						return (T) new TransactionalValue((Long) values.get(config.getValueColumns().length), // txid
							values.subList(0, config.getValueColumns().length)); // curr values
					default:
						return (T) values;
					}
				}
			}
		});
	}

	/**
	 * multiput implementation for mongodb
	 * 
	 */
	@Override
	public void multiPut(final List<List<Object>> keys, final List<T> values) {
		// build a query insert into table(key1, key2, ..., value1, value2, ... , [txid], [prev_val1], ...) values (?,?,...), ... ON DUPLICATE KEY UPDATE value1 = VALUES(value1), ...
		// how many params per row of data
		// opaque => keys + 2 * vals + 1
		// transactional => keys + vals + 1
		// non-transactional => keys + vals
		int paramCount = 0;
		switch (config.getType()) {
		case OPAQUE:
			paramCount += config.getValueColumns().length;
		case TRANSACTIONAL:
			paramCount += 1;
		default:
			paramCount += (config.getKeyColumns().length + config.getValueColumns().length);
		}
		final StringBuilder queryBuilder = new StringBuilder().append("INSERT INTO ")
			.append(config.getTable())
			.append("(")
			.append(buildColumns())
			.append(") VALUES ")
			.append(Joiner.on(",").join(repeat("(" + Joiner.on(",").join(repeat("?", paramCount)) + ")", keys.size())))
			.append(" ON DUPLICATE KEY UPDATE ")
			.append(Joiner.on(",").join(Lists.transform(getValueColumns(), new Function<String, String>() {
				@Override
				public String apply(final String col) {
					return col + " = VALUES(" + col + ")";
				}
			}))); // for every value column, constructs "valcol = VALUE(valcol)", joined by commas
		// run the update
		final List<Object> params = flattenPutParams(keys, values);
		PreparedStatement ps = null;
		int i = 0;
		try {
			ps = connection.prepareStatement(queryBuilder.toString());
			for (final Object param : params) {
				ps.setObject(++i, param);
			}
			ps.execute();
		} catch (final SQLException ex) {
			logger.error("Multiput update failed", ex);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException ex) {
					// don't care
				}
			}
		}
		logger.debug(String.format("%1$d keys flushed", keys.size()));
	}

	private String buildColumns() {
		final List<String> cols = Lists.newArrayList(config.getKeyColumns()); // the columns for the composite unique key
		cols.addAll(getValueColumns());
		return Joiner.on(",").join(cols);
	}

	private String buildKeyQuery(final int n) {
		final String single = "(" + Joiner.on(" AND ").join(Lists.transform(Arrays.asList(config.getKeyColumns()), new Function<String, String>() {
			@Override
			public String apply(final String field) {
				return field + " = ?";
			}
		})) + ")";
		return Joiner.on(" OR ").join(repeat(single, n));
	}

	private List<String> getValueColumns() {
		final List<String> cols = Lists.newArrayList(config.getValueColumns()); // the columns storing the values
		if (StateType.OPAQUE.equals(config.getType()) || StateType.TRANSACTIONAL.equals(config.getType())) {
			cols.add("txid");
		}
		if (StateType.OPAQUE.equals(config.getType())) {
			cols.addAll(Lists.transform(Arrays.asList(config.getValueColumns()), new Function<String, String>() {
				@Override
				public String apply(final String field) {
					return "prev_" + field;
				}
			})); // the prev_* columns
		}
		return cols;
	}

	/**
	 * run the multi get query, passing in the list of keys and returning key tuples mapped to value tuples
	 * 
	 * @param sql
	 * @param keys
	 * @return
	 */
	private Map<List<Object>, List<Object>> query(final String sql, final List<List<Object>> keys) {
		final Map<List<Object>, List<Object>> result = new HashMap<>();
		PreparedStatement ps = null;
		int i = 0;
		try {
			ps = connection.prepareStatement(sql);
			for (final List<Object> key : keys) {
				for (final Object keyPart : key) {
					ps.setObject(++i, keyPart);
				}
			}
			final ResultSet rs = ps.executeQuery();
			final Function<String, Object> rsReader = new Function<String, Object>() {
				@Override
				public Object apply(final String column) {
					try {
						return rs.getObject(column);
					} catch (final SQLException sqlex) {
						return null;
					}
				}
			};
			final List<String> keyColumns = Arrays.asList(config.getKeyColumns());
			final List<String> valueColumns = getValueColumns();
			while (rs.next()) {
				result.put(Lists.transform(keyColumns, rsReader), Lists.transform(valueColumns, rsReader));
			}
			rs.close();
		} catch (final SQLException ex) {
			logger.error("multiget query failed", ex);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException ex) {
					// don't care
				}
			}
		}
		return result;
	}

	@SuppressWarnings("rawtypes")
	private List<Object> flattenPutParams(final List<List<Object>> keys, final List<T> values) {
		final List<Object> flattenedRows = new ArrayList<>();
		for (int i = 0; i < keys.size(); i++) {
			flattenedRows.addAll(keys.get(i));
			switch (config.getType()) {
			case OPAQUE:
				flattenedRows.addAll(valueToParams(((OpaqueValue) values.get(i)).getCurr()));
				flattenedRows.add(((OpaqueValue) values.get(i)).getCurrTxid());
				flattenedRows.addAll(valueToParams(((OpaqueValue) values.get(i)).getPrev()));
				break;
			case TRANSACTIONAL:
				flattenedRows.addAll(valueToParams(((TransactionalValue) values.get(i)).getVal()));
				flattenedRows.add(((TransactionalValue) values.get(i)).getTxid());
				break;
			default:
				flattenedRows.addAll(valueToParams(values.get(i)));
			}
		}
		return flattenedRows;
	}

	@SuppressWarnings("unchecked")
	private List<Object> valueToParams(final Object value) {
		if (!(value instanceof List)) {
			return repeat(value, config.getValueColumns().length);
		} else {
			return (List<Object>) value;
		}
	}

	private <U> List<U> repeat(final U val, final int count) {
		final List<U> list = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			list.add(val);
		}
		return list;
	}

	@SuppressWarnings("serial")
	static class Factory implements StateFactory {
		private MysqlStateConfig config;

		Factory(final MysqlStateConfig config) {
			this.config = config;
		}

		@Override
		@SuppressWarnings({"rawtypes","unchecked"})
		public State makeState(final Map conf, final IMetricsContext context, final int partitionIndex, final int numPartitions) {
			final CachedMap map = new CachedMap(new MysqlState(config), config.getCacheSize());
			switch (config.getType()) {
			case OPAQUE:
				return OpaqueMap.build(map);
			case TRANSACTIONAL:
				return TransactionalMap.build(map);
			default:
				return NonTransactionalMap.build(map);
			}
		}
	}
}
