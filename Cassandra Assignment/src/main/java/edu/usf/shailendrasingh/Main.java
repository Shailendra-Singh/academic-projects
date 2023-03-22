package edu.usf.shailendrasingh;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class Main {
    public static Scanner scanner;

    public static void main(String[] args) {
        System.out.println("Cassandra Assignment - by Shailendra Singh");
        CassandraConnector connector = new CassandraConnector();
        System.out.println(connector.getSessionStatus());

        scanner = new Scanner(System.in);
        System.out.print("Enter Cassandra Table Name: ");
        String tableName = scanner.next();
        BookRepository repository = new BookRepository(tableName, connector);

        System.out.println("[Options] -> c: Create, r: Read, i: Insert, u:Update, d:Delete, e:EXIT\n");
        System.out.print("Option:\t");
        char c = scanner.next().toLowerCase().charAt(0);
        while (c != 'e') {
            switch (c) {
                case 'c':
                    createOperation(repository);
                    break;
                case 'i':
                    insertOperation(repository);
                    break;
                case 'r':
                    readOperation(repository);
                    break;
                case 'u':
                    updateOperation(repository);
                    break;
                case 'd':
                    deleteOperation(repository);
                    break;
                default:
                    System.out.println("Wrong option selected!");
            }

            System.out.println("[Options] -> c: Create, r: Read, i: Insert, u:Update, d:Delete, e:EXIT\n");
            System.out.print("Option:\t");
            c = scanner.next().toLowerCase().charAt(0);
        }

        connector.close();
        System.out.println("Connection Closed! Good Bye!");
    }

    public static void createOperation(BookRepository repository) {
        // CREATE
        boolean result = repository.createTable();
        System.out.printf("Create Success: %s\n", result);
    }

    public static void insertOperation(BookRepository repository) {
        // INSERT
        Book b1 = new Book(UUID.randomUUID(),
                "Beyond Good and Evil",
                "Friedrich Nietzsche",
                1886);

        Book b2 = new Book(UUID.randomUUID(),
                "Notes from Underground",
                "Fyodor Dostoevsky",
                1864);

        Book b3 = new Book(UUID.randomUUID(),
                "Pride and Prejudice",
                "Jane Austen",
                1813);

        Book b4 = new Book(UUID.randomUUID(),
                "Great Expectations",
                "Charles Dickens",
                1861);

        Book b5 = new Book(UUID.randomUUID(),
                "Julius Caesar",
                "William Shakespeare",
                2023);

        Book b6 = new Book(UUID.randomUUID(),
                "The Great Gatsby",
                "F. Scott Fitzgerald",
                1925);

        List<Book> books = new ArrayList<>();
        books.add(b1);
        books.add(b2);
        books.add(b3);
        books.add(b4);
        books.add(b5);
        books.add(b6);

        for (Book book : books) {
            UUID result = repository.insertBook(book);
            System.out.printf("Inserted book with Id: %s\n", result);
        }
    }

    public static void readOperation(BookRepository repository) {
        List<Book> books = repository.selectAll();
        for (Book book : books)
            System.out.println(book);
    }

    public static void updateOperation(BookRepository repository) {
        System.out.println("Enter valid year for 'Julius Caesar' book.");
        int year = scanner.nextInt();
        Book targetBook = repository
                .selectAll()
                .stream()
                .filter(book -> book.title().equals("Julius Caesar"))
                .findFirst().get();

        boolean result = repository.updateBook(targetBook.id(),
                targetBook.title(),
                targetBook.author(),
                year);
        System.out.printf("Update Success: %s\n", result);
    }

    public static void deleteOperation(BookRepository repository) {
        System.out.println("Enter valid UUID for book to be deleted.");
        UUID uuid = UUID.fromString(scanner.next());
        boolean result = repository.deleteBook(uuid);
        System.out.printf("Delete Success: %s\n", result);
    }
}