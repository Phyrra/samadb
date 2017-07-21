package ch.sama.db.query;

import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;

import java.util.List;

public interface IStatement {
    List<List<Tupel>> execute();
    DataContext getContext();
    DataContext getFilteredContext(DataContext context);
}
