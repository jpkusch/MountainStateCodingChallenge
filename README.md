I. Purpose

II. Getting Started

III. Design
    a. Dependencies
        This project uses two outside resources to help in its job, SQLite-JDBC and Maven.
        I chose to use Maven to make running this project consistent and quick between system
        and I used SQLite-JDBC to give myself a reasonable interface to run SQLite queries.

        <MAKE LATER> Talk of CSVParser / if I change to Opencsv </MAKE LATER>

    b. Design
        - Driver.java
            The first decision for this file was using command line input.
            It works well with the single input this program needs, but I
            would like to go back to choosing not to have the file extension
            be part of the input. It makes creating the output files easier, 
            but I worry about how intuitive it is for a user.
            
            The isValid function ended up here because it didn't really fit
            with the other two classes and wasn't enough functionality to
            merit its own class. CSVParser and Distributor serve to keep a 
            short main function and abstract File IO away from the driver.

        -CSVParser.java
            EXPLAIN THE REGEX

        -Distributor.java
            The main structural challenge for this class was tracking the three
            output files well. I decided that the best way to do that was to
            make the class's main job tracking three file streams while the mess
            of setup was left in the constructor. 

            The write method ...
