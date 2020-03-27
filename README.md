I. Purpose

    This repo holds my solution to the 2019 jr. coding challenge for Mountain
    Side Software Solutions. It's a display of how I solve software issues
    for the purposes of the hiring process.

II. Getting Started

    I built this project using IntelliJ idea to run it. If you wish to use Maven
    I did provide a skeleton .pom file to use for setup.
    
III. Design

    a. Dependencies
        I ended up just using sqlite-jdbc so that I could have an interface for
        SQL queries. At the moment, I'm not completely familiar with Maven, so 
        it ended up being better for me to focus on completing the work than
        learning that skill due to the short time frame.

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
            I ended up deciding to write my own parser due to it being a fairly
            simple piece of code to create and not wanting to make more
            dependencies to outside libraries.

        -Distributor.java
            The main structural challenge for this class was tracking the three
            output files well. I decided that the best way to do that was to
            make the class's main job tracking three file streams while the mess
            of setup was left in the constructor. 

            For efficiency, I ended up choosing to make one long SQL query
            instead of doing an INSERT query every time I ran the write command.
            Reconnecting to the database each time had about tripled my runtime.
