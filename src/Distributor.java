import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Distributor {
  private BufferedWriter badStream;
  private BufferedWriter logStream;
  private String dbPath;
  //The application was wasting time with multiple queries, so I'm making it one
  private String query;

  /**
   * Constructs a distributor to write records to <filename>.db
   * and <filename>-bad.csv and <filename>.log
   *
   * @param filename Name of the files to output records to
   */
  Distributor(String filename) throws IOException {
    //Generates file paths starting at /MountainSide
    String csvPath = System.getProperty("user.dir") + System.getProperty("file.separator")  +
        "output" + System.getProperty("file.separator") + filename + "-bad.csv";

    dbPath = "jdbc:sqlite:" + System.getProperty("user.dir") + System.getProperty("file.separator") +
        "output" + System.getProperty("file.separator") + filename + ".db";

    String logPath = System.getProperty("user.dir") + System.getProperty("file.separator") +
        "output" + System.getProperty("file.separator") + filename + ".log";

    File csvOut = new File(csvPath);
    File logOut = new File(logPath);
    File dbFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator") +
        "output" + System.getProperty("file.separator") + filename + ".db");

    if (csvOut.exists()) {
      csvOut.delete();
    }
    if (logOut.exists()) {
      logOut.delete();
    }
    if (dbFile.exists()) {
      dbFile.delete();
    }
    csvOut.createNewFile();
    logOut.createNewFile();
    badStream = new BufferedWriter(new FileWriter(csvOut));
    logStream = new BufferedWriter(new FileWriter(logOut));

    try (Connection dbConn = DriverManager.getConnection(dbPath)) {
      Statement stmt = dbConn.createStatement();
      stmt.execute("CREATE TABLE Data (A VARCHAR(255)" +
          ", B VARCHAR(255), C VARCHAR(255), D VARCHAR(255), E VARCHAR(255)" +
          ", F VARCHAR(255), G VARCHAR(255), H VARCHAR(255), I VARCHAR(255)" +
          ", J VARCHAR(255));");
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }

    query = "INSERT INTO Data VALUES";
  }

  /**
   * Writes records to the correct file
   *
   * @param record Database values to be written to files
   * @param isValid boolean that states whether a record is bad and must be
   *                written to <filename>-bad.csv or is good and should be
   *                written to <filename>.db
   */
  void write(String[] record, boolean isValid) throws IOException, SQLException {
    if(!isValid) {
      String toWrite = "";
      for (String entry: record) {
        toWrite = toWrite + entry + ", ";
      }
      toWrite = toWrite.substring(0, toWrite.length() - 2);
      badStream.write(toWrite);
      badStream.newLine();
    }
    else {
      String statement = " (";

      for (String entry: record) {

        //doing a second search through every string feels inefficient, see if I can do this at the split
        entry = entry.replace("'", "''");
        statement = statement + "'" + entry + "', ";
      }
      statement = statement.substring(0, statement.length() - 2);
      statement = statement + "),";
      query = query + statement;
      //stmt.execute(query);
    }
  }

  /**
   * Writes the log data to its output file
   *
   * @param numRecords The number of records processed
   * @param numValidRecords The number of valid records sent to the database
   * @param numBadRecords The number of invalid records sent to csv
   */
  void log(int numRecords, int numValidRecords, int numBadRecords) {
    try {
      query = query.substring(0, query.length() - 1) + ";";
      Connection dbConn =  DriverManager.getConnection(dbPath);
      Statement stmt = dbConn.createStatement();
      stmt.execute(query);
      logStream.write(numRecords + " Records Received");
      logStream.newLine();
      logStream.write(numValidRecords + " Records Successful");
      logStream.newLine();
      logStream.write(numBadRecords + " Records Failed");

      badStream.close();
      logStream.close();
    } catch (IOException | SQLException e) {
      System.out.println(e.getMessage());
    }
  }
}