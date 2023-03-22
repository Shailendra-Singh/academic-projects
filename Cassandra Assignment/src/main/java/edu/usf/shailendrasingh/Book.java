package edu.usf.shailendrasingh;

import java.util.UUID;
public record Book(UUID id, String title, String author, int year) {

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("UUID:\t%s\n", this.id()));
        sb.append(String.format("Title:\t%s\n", this.title()));
        sb.append(String.format("Author:\t%s\n", this.author()));
        sb.append(String.format("Year:\t%d\n", this.year()));
        sb.append("\n");
        return sb.toString();
    }
}