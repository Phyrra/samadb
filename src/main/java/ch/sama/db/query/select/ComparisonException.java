package ch.sama.db.query.select;

public class ComparisonException extends RuntimeException {
	public ComparisonException(Object lhs, Object rhs) {
		super("Cannot compare {" + getString(lhs) + "} to {" + getString(rhs) + "}");
	}

	private static String getString(Object o) {
		return o == null ? "null" : o.toString();
	}
}
