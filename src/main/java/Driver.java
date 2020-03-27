import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;

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
  public static void main(String[] args) {
    String csvPath = System.getProperty("user.dir") + System.getProperty("file.separator") +
        "input" + System.getProperty("file.separator") + args[0] + ".csv";
    Distributor distr = new Distributor(args[0]);
    processValues(csvPath, distr);
  }

  /**
   * Reads csvFile and sends it to the distributor for writing
   * 
   * @param csvPath a file path corresponding to the input csv file
   * @param distr a distrubutor that writes values to their output files
   */
 static void processValues(String csvPath, Distributor distr) {
    int numRecords = 0;
    int numValidRecords = 0;
    int numBadRecords = 0;
    boolean validLine = true;
    try {
      CSVReader reader = new CSVReader(new FileReader(csvPath));
      //Only need columns from the first line, it is not data
      String[] values = reader.readNext();
      int numColumns = values.length;

      //for each line of the input file, check if it's valid and write it to the correct file
      values = reader.readNext();
      while (values != null) {
        validLine = isValid(values, numColumns);
        distr.write(values, validLine);
        
        if (validLine) {
          ++numValidRecords;
        } else {
          ++numBadRecords;
        }
        ++numRecords;

        values = reader.readNext();
      }
    } catch (CsvValidationException | IOException e) {
      System.out.println(e.getMessage());
      System.exit(1);
    }
    distr.pushToDB();
    distr.log(numRecords, numValidRecords, numBadRecords);
  }

  /**
   * Determines whether a record fills all required fields
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