package edu.usf.shailendrasingh;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BookRepository {
    private final String tableName;
    private final CassandraConnector connector;

    public BookRepository(String tableName, CassandraConnector connector) {
        this.tableName = tableName;
        this.connector = connector;
    }

    public boolean createTable() {
        CreateTable createTable = SchemaBuilder.createTable(this.tableName)
                .withPartitionKey("Id", DataTypes.UUID)
                .withColumn("Title", DataTypes.TEXT)
                .withColumn("Author", DataTypes.TEXT)
                .withColumn("Year", DataTypes.INT);

        ResultSet queryResult = executeStatement(createTable.build(), this.connector.getKeyspace());
        return queryResult.wasApplied();
    }

    private ResultSet executeStatement(SimpleStatement statement, String keyspace) {
        if (keyspace != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspace));
        }

        return this.connector.getSession().execute(statement);
    }

    private boolean executeStatement(String query) {
        ResultSet queryStatus = this.connector.getSession().execute(query);
        return queryStatus.wasApplied();
    }

    public UUID insertBook(Book book) {
        String insertQuery = "INSERT INTO %1$s (Id, Title, Author, Year) VALUES (%2$s,'%3$s','%4$s',%5$s);";
        insertQuery = String.format(insertQuery,
                this.tableName,
                book.id(),
                book.title(),
                book.author(),
                book.year()
        );
        executeStatement(insertQuery);
        return book.id();
    }

    public boolean updateBook(UUID uuid, String title, String author, int year) {
        String updateQuery = "UPDATE %1$s SET Title='%2$s',Author='%3$s',Year=%4$s WHERE Id=%5$s;";
        updateQuery = String.format(updateQuery, this.tableName, title, author, year, uuid);
        return executeStatement(updateQuery);
    }

    public boolean deleteBook(UUID uuid) {
        String deleteQuery = "DELETE FROM %1$s WHERE Id=%2$s;";
        deleteQuery = String.format(deleteQuery, this.tableName, uuid);
        return executeStatement(deleteQuery);
    }

    public List<Book> selectAll() {
        Select select = QueryBuilder.selectFrom(this.tableName).all();
        ResultSet resultSet = executeStatement(select.build(), this.connector.getKeyspace());
        List<Book> result = new ArrayList<>();
        resultSet.forEach(x -> result.add(
                new Book(x.getUuid("Id"),
                        x.getString("Title"),
                        x.getString("Author"),
                        x.getInt("Year"))
        ));
        return result;
    }
}
