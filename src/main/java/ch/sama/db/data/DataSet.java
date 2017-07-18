package ch.sama.db.data;

import java.util.ArrayList;
import java.util.List;

public class DataSet {
    private List<DataRow> rows;

    public DataSet() {
        this.rows = new ArrayList<>();
    }

    public List<DataRow> getRows() {
        return rows;
    }

    public void addRow(DataRow row) {
        rows.add(row);
    }
}
