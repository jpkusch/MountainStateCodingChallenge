import java.io.IOException;
import java.sql.SQLException;

/**
 * A driver class holding the main method for this application
 *
 * MountainParser takes a CSV file as input and transforms it into a database
 * containing all valid records and a CSV file containing all invalid records
 */
public class Driver {
  /**
   * Main method for this application
   *
   * @param - args, The name of the file to convert in the 'input' directory
   *          NOTE: DO NOT add the file extension
   */
  public static void main(String[] args) throws IOException, SQLException {

    int numRecords = 0;
    int numValidRecords = 0;
    int numBadRecords = 0;

    CSVParser parser = new CSVParser(args[0]);
    Distributor distr = new Distributor(args[0]);
    boolean validLine = true;

    //Get the columns from the first line of the file
    String[] values = parser.readLine();
    int numColumns = values.length;

    values = parser.readLine();
    while (values != null) {
      validLine = isValid(values, numColumns);
      if (validLine) {
        ++numValidRecords;
      } else {
        ++numBadRecords;
      }
      ++numRecords;

      distr.write(values, validLine);
      values = parser.readLine();
    }
    distr.log(numRecords, numValidRecords, numBadRecords);
  }

  /**
   * Determines whether a record fulfills all required fields
   *
   * @param record The records to be checked
   * @return true if the records contain values in all fields, false otherwises
   */
  static boolean isValid(String[] record, int numColumns) {
    if (record.length != numColumns) {
      return false;
    }
    for(String item: record) {
      if (item.isEmpty()) {
        return false;
      }
    }
    return true;
  }
}