import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class BaseTable {
    private String dir = "db";
    private String tableName;
    private HashMap<String, Long> metadata;
    private String filePrefix = dir + "/" + tableName;

    public BaseTable(String tableName) {
        this.tableName = tableName;
        this.metadata = new HashMap<>();
        this.filePrefix = dir + "/" + tableName;
    }

    public void insert(String key, String value) {
        try (RandomAccessFile dataFile = new RandomAccessFile(filePrefix + ".data", "rw");
                PrintWriter metadataWriter = new PrintWriter(new FileWriter(filePrefix + ".meta", true))) {
            long offset = dataFile.length();
            metadata.put(key, offset);
            metadataWriter.println(key + ":" + offset);
            dataFile.seek(offset);
            dataFile.write(value.getBytes());
            dataFile.write(System.lineSeparator().getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String search(String key) {
        Long offset = metadata.get(key);
        if (offset != null) {
            try (RandomAccessFile dataFile = new RandomAccessFile(filePrefix + ".data", "r")) {
                dataFile.seek(offset);
                return dataFile.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // Other functions for updating, deleting, and more can be added as needed.
}
