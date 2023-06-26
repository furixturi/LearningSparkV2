This directory contains all the Scala and Python examples for Chapter 2, along with the code to generate the M&Ms data set.

To build and run these examples cd in the respective scala or py/src directory and follow the instructions in the relevant README.md files.

## How to change the default log level to WARN

The default log level is the verbose `info`. To change it to `warn`:

- Go to $SPARK_HOME/conf
- Edit the file `log4j2.properties.template`
  - Find the line `rootLogger.level = info` and change it to `rootLogger.level = warn`
- Save the edited file as `log4j2.properties`
