import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class CSVParser {
  private String[] currentLine;
  private BufferedReader inputFile;
  private String lineHolder;

  /**
   * Constructs a parser for a given csv file
   *
   * @param filename name of a valid csv file in this directory
   */
  CSVParser(String filename) throws FileNotFoundException {
    try {
      String filePath = System.getProperty("user.dir") + System.getProperty("file.separator") +
          "input" + System.getProperty("file.separator") + filename + ".csv";

      inputFile = new BufferedReader(new FileReader(filePath));
    } catch (FileNotFoundException except) {
      System.out.println(except.getMessage());
      System.exit(1);
    }
  }

  /**
   * Reads the next line from a CSV file and returns it, closes the file when done
   *
   * @return An array of strings that is the next line of the CSV file
   */
  String[] readLine() {
    try {
      lineHolder = inputFile.readLine();
    } catch (IOException badIo) {
      System.out.println("Error reading file.");
      System.exit(1);
    }

    if (lineHolder != null) {
      //splits the line on commas not contained in quotes
      currentLine = lineHolder.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      return currentLine;
    }
    else {
      return null;
    }
  }
}
