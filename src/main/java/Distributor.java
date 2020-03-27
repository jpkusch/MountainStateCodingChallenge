import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Distributor {
  private BufferedWriter badStream;
  private BufferedWriter logStream;
  private CSVWriter badWriter;
  private String dbPath;
  private StringBuilder query;

  /**
   * Constructs a distributor to write records to <filename>.db
   * and <filename>-bad.csv and <filename>.log
   *
   * @param filename Name of the files to output records to
   */
  Distributor(String filename) {
    String outDir = System.getProperty("user.dir") + System.getProperty("file.separator")  +
        "output";
    String csvPath = outDir + System.getProperty("file.separator") + filename + "-bad.csv";
    dbPath = "jdbc:sqlite:" + outDir +  System.getProperty("file.separator") + filename + ".db";
    String logPath = outDir + System.getProperty("file.separator") + filename + ".log";

    File csvOut = new File(csvPath);
    File logOut = new File(logPath);
    File dbFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator") +
        "output" + System.getProperty("file.separator") + filename + ".db");
    File outDirectory = new File(outDir);
    if (!outDirectory.exists()) {
      outDirectory.mkdir();
    }

    if (csvOut.exists()) {
      csvOut.delete();
    }
    if (logOut.exists()) {
      logOut.delete();
    }
    if (dbFile.exists()) {
      dbFile.delete();
    }
    try {
      csvOut.createNewFile();
      logOut.createNewFile();
      badStream = new BufferedWriter(new FileWriter(csvOut));
      logStream = new BufferedWriter(new FileWriter(logOut));
      badWriter = new CSVWriter(badStream);
      String[] line = {"A","B","C","D","E","F","G","H","I","J"};
      badWriter.writeNext(line);
      //badStream.write("A, B, C, D, E, F, G, H, I, J");
      //badStream.newLine();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
    query = new StringBuilder("INSERT INTO Data VALUES");
  }

  /**
   * Writes records to the correct file
   *
   * @param record Database values to be written to files
   * @param isValid boolean that states whether a record is bad and must be
   *                written to <filename>-bad.csv or is good and should be
   *                written to <filename>.db
   */
  void write(String[] record, boolean isValid){
    if (!isValid) {
      badWriter.writeNext(record);
      /**
       String toWrite = "";
       for (String entry: record) {
         try {
           badStream.write(entry);
         } catch (IOException e) {
           System.out.println(e.getMessage());
           System.exit(1);
         }
        toWrite = toWrite + entry + ", ";
       }
       toWrite = toWrite.substring(0, toWrite.length() - 2);
       try {
        //badStream.write(toWrite);
        badStream.newLine();
       } catch (IOException e) {
        System.out.println(e.getMessage());
        System.exit(1);
       }
       */
    } else {
      StringBuilder statement = new StringBuilder(" (");
      for (String entry : record) {
        //need to escape the quote marks in these entries
        entry = entry.replace("'", "''");
        statement.append("'");
        statement.append(entry);
        statement.append("', ");
      }
      statement.setLength(statement.length() - 2);
      statement.append("),");
      query = query.append(statement.toString());
      /**
       String statement = " (";
       for (String entry : record) {
       //need to escape the quote marks in these entries
       entry = entry.replace("'", "''");
       statement = statement + "'" + entry + "', ";
       }
       statement = statement.substring(0, statement.length() - 2);
       statement = statement + "),";
       query = query + statement;
       */
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
      query.setLength(query.length() - 1);
      query.append(";");
      //the reason the table is created here is to be efficient
      //I don't want to connect to the database twice
      try {
        Connection dbConn = DriverManager.getConnection(dbPath);
        Statement stmt1 = dbConn.createStatement();
        stmt1.execute("CREATE TABLE Data (A VARCHAR(255)" +
            ", B VARCHAR(255), C VARCHAR(255), D VARCHAR(255), E VARCHAR(255)" +
            ", F VARCHAR(255), G VARCHAR(255), H VARCHAR(255), I VARCHAR(255)" +
            ", J VARCHAR(255));");
        Statement stmt = dbConn.createStatement();
        stmt.execute(query.toString());
      } catch (SQLException e) {
        System.out.println(e.getMessage());
        System.exit(1);
      }
      logStream.write(numRecords + " Records Received");
      logStream.newLine();
      logStream.write(numValidRecords + " Records Successful");
      logStream.newLine();
      logStream.write(numBadRecords + " Records Failed");

      badStream.close();
      logStream.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}