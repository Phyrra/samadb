package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.Table;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

class JoinBuilder {
	enum Type {
		INNER,
		LEFT,
		RIGHT
	}

	private interface IJoinBuilder {
		DataContext buildContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter);
	}

	private class InnerJoin implements IJoinBuilder {
		private Datastore datastore;
		private DataContext context;
		private String table;

		InnerJoin(Datastore datastore, DataContext context, String table) {
			this.datastore = datastore;
			this.context = context;
			this.table = table;
		}

		@Override
		public DataContext buildContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
			if (!datastore.hasTable(table)) {
				throw new UnknownTableException(table);
			}

			DataSet tableData = datastore.getData(table);
			context.registerAlias(alias, datastore.getTable(table));

			List<Map<String, Map<String, Object>>> joined = new ArrayList<>();

			context.getData().forEach(row -> {
				tableData.getRows().stream()
						.map(DataRow::toMap)
						.map(rowMap -> {
							Map<String, Map<String, Object>> newSet = new HashMap<>();

							// Add existing data
							row.keySet()
									.forEach(key -> newSet.put(key, row.get(key)));

							// Add new data
							newSet.put(alias, rowMap);

							return newSet;
						})
						.filter(filter::apply)
						.forEach(joined::add);
			});

			return new DataContext(
					context.getKnownAliases(),
					joined
			);
		}
	}

	private class LeftJoin implements IJoinBuilder {
		private Datastore datastore;
		private DataContext context;
		private String table;

		LeftJoin(Datastore datastore, DataContext context, String table) {
			this.datastore = datastore;
			this.context = context;
			this.table = table;
		}

		@Override
		public DataContext buildContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
			if (!datastore.hasTable(table)) {
				throw new UnknownTableException(table);
			}

			DataSet tableData = datastore.getData(table);
			context.registerAlias(alias, datastore.getTable(table));

			List<Map<String, Map<String, Object>>> joined = new ArrayList<>();

			context.getData().forEach(row -> {
				List<Map<String, Map<String, Object>>> results = tableData.getRows().stream()
						.map(DataRow::toMap)
						.map(rowMap -> {
							Map<String, Map<String, Object>> newSet = new HashMap<>();

							// Add existing data
							row.keySet()
									.forEach(key -> newSet.put(key, row.get(key)));

							// Add new data
							newSet.put(alias, rowMap);

							return newSet;
						})
						.filter(filter::apply)
						.collect(Collectors.toList());

				if (!results.isEmpty()) {
					joined.addAll(results);
				} else {
					Map<String, Map<String, Object>> newSet = new HashMap<>();
					row.keySet()
							.forEach(key -> newSet.put(key, row.get(key)));

					joined.add(newSet);
				}
			});

			return new DataContext(
					context.getKnownAliases(),
					joined
			).fill();
		}
	}

	private class RightJoin implements IJoinBuilder {
		private Datastore datastore;
		private DataContext context;
		private String table;

		RightJoin(Datastore datastore, DataContext context, String table) {
			this.datastore = datastore;
			this.context = context;
			this.table = table;
		}

		@Override
		public DataContext buildContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
			if (!datastore.hasTable(table)) {
				throw new UnknownTableException(table);
			}

			DataSet tableData = datastore.getData(table);
			context.registerAlias(alias, datastore.getTable(table));

			List<Map<String, Map<String, Object>>> data = context.getData();

			List<Map<String, Map<String, Object>>> joined = new ArrayList<>();

			tableData.getRows().stream()
					.map(DataRow::toMap)
					.forEach(rowMap -> {
						List<Map<String, Map<String, Object>>> results = data.stream()
								.map(row -> {
									Map<String, Map<String, Object>> newSet = new HashMap<>();

									// Add existing data
									row.keySet()
											.forEach(key -> newSet.put(key, row.get(key)));

									// Add new data
									newSet.put(alias, rowMap);

									return newSet;
								})
								.filter(filter::apply)
								.collect(Collectors.toList());

						if (!results.isEmpty()) {
							joined.addAll(results);
						} else {
							Map<String, Map<String, Object>> newSet = new HashMap<>();
							newSet.put(alias, rowMap);

							joined.add(newSet);
						}
					});

			return new DataContext(
					context.getKnownAliases(),
					joined
			).fill();
		}
	}

	private IJoinBuilder joinBuilder;

	JoinBuilder(Type type, Datastore datastore, DataContext context, String table) {
		this.joinBuilder = getBuilder(type, datastore, context, table);
	}

	private IJoinBuilder getBuilder(Type type, Datastore datastore, DataContext context, String table) {
		switch (type) {
			case INNER:
				return new InnerJoin(datastore, context, table);
			case LEFT:
				return new LeftJoin(datastore, context, table);
			case RIGHT:
				return new RightJoin(datastore, context, table);
		}

		return null;
	}

	JoinBuilder(Datastore datastore, DataContext context, String table) {
		this(Type.INNER, datastore, context, table);
	}

	DataContext buildContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
		return joinBuilder.buildContext(alias, filter);
	}
}
