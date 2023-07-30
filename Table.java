public class Table extends BaseTable {

    public Table(String tableName) {
        super(tableName);
    }

    public static void main(String[] args) {
        Table myTable = new Table("my_table");

        myTable.insert("id1", "John Doe");
        myTable.insert("id2", "Jane Smith");

        System.out.println("Search for 'id1': " + myTable.search("id1"));
        System.out.println("Search for 'id2': " + myTable.search("id2"));
    }
}
