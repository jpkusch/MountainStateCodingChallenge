import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Distributor {
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
    String outDir = System.getProperty("user.dir") + System.getProperty("file.separator")  + "output";
    File outDirectory = new File(outDir);
    if (!outDirectory.exists()) {
      outDirectory.mkdir();
    }

    String csvPath = makeNewFile(outDir, filename, "-bad.csv");
    String logPath = makeNewFile(outDir, filename, ".log");
    String dbFilePath = makeNewFile(outDir, filename, ".db");

    //Sets up file writers
    dbPath = "jdbc:sqlite:" + dbFilePath;
    query = new StringBuilder("INSERT INTO Data VALUES");
    try {
      logStream = new BufferedWriter(new FileWriter(logPath));
      badWriter = new CSVWriter(new BufferedWriter(new FileWriter(csvPath)));
      String[] line = {"A","B","C","D","E","F","G","H","I","J"};
      badWriter.writeNext(line);

    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }

  /**
   * Deletes old versions of output files and creates new versions
   * 
   * @param dirPath path to the output directory
   * @param filename name of file to be created
   * @param extension file extension we wish to use
   */
  private String makeNewFile(String dirPath, String filename, String extension) {
    StringBuilder path = new StringBuilder(dirPath);
    path.append(System.getProperty("file.separator"));
    path.append(filename);
    path.append(extension);
    File maker = new File(path.toString());
    if (maker.exists()) {
      maker.delete();
    }
    try {
      maker.createNewFile();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
    return path.toString();
  }

  /**
   * Writes records to the correct file
   *
   * @param record Database values to be written to files
   * @param isValid boolean that states whether a record is bad and must be
   *                written to <filename>-bad.csv or is good and should be
   *                set to write to <filename>.db
   */
  void write(String[] record, boolean isValid){
    if (!isValid) {
      badWriter.writeNext(record);
    } 
    else {
      StringBuilder statement = new StringBuilder(" (");
      for (String entry : record) {
        //Escape quote marks so they are written to the final SQL
        entry = entry.replace("'", "''");
        statement.append("'");
        statement.append(entry);
        statement.append("', ");
      }
      statement.setLength(statement.length() - 2);
      statement.append("),");
      query = query.append(statement.toString());
    }
  }

  /**
   * Pushes results to the Database
   * NOTE: This is done separately to avoid reconnecting to the database
   */
  void pushToDB() {
    //fix last entry from asking for next entry to ending the query
    query.setLength(query.length() - 1);
    query.append(";");
    try {
      Connection dbConn = DriverManager.getConnection(dbPath);
      Statement stmt1 = dbConn.createStatement();
      stmt1.execute("CREATE TABLE Data (A VARCHAR(255)" +
          ", B VARCHAR(255), C VARCHAR(255), D VARCHAR(255), E VARCHAR(255)" +
          ", F VARCHAR(255), G VARCHAR(255), H VARCHAR(255), I VARCHAR(255)" +
          ", J VARCHAR(255));");

      Statement stmt2 = dbConn.createStatement();
      stmt2.execute(query.toString());
    } catch (SQLException e) {
      System.out.println(e.getMessage());
      System.exit(1);
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
      logStream.write(numRecords + " Records Received\n");
      logStream.write(numValidRecords + " Records Successful\n");
      logStream.write(numBadRecords + " Records Failed\n");
      logStream.close();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
  }
}