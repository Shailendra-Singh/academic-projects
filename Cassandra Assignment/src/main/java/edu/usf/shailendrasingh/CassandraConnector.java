package edu.usf.shailendrasingh;

import com.datastax.oss.driver.api.core.CqlSession;

public class CassandraConnector {
    private final CqlSession session;

    public CassandraConnector() {
        this.session = CqlSession.builder().build();
    }

    public String getSessionStatus() {
        var metadata = this.session.getMetadata();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Cluster Name:\t%s\n", metadata.getClusterName().get()));
        sb.append(String.format("Keyspace:\t%s\n", "usf"));
        sb.append(String.format("Is Connected:\t%s\n", this.isOpen()));
        return sb.toString();
    }

    public CqlSession getSession() {
        return this.session;
    }

    public String getKeyspace() {
        if (this.session.getKeyspace().isPresent())
            return this.session.getKeyspace().get().toString();

        return null;
    }

    public boolean isOpen() {
        return !this.session.isClosed();
    }

    public void close() {
        this.session.close();
    }
}