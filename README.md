piggy
=====

Pig UDFS in Java

Follow the following steps for creating a jar file which can be used in your pig scripts:


Download the jar libs from the lib directory
Copy the source and the jars in a folder called i.e. date_utils
Compile the code in date_utils javac -cp hadoop.jar:pig.jar *.java
Create the jar file by running jar -cf date_utils.jar date_utils/


You can define the function in pig like:

DEFINE ConvertDate date_utils.ConvertDateFormat();

date_utils.ConvertDateFormat() means that there is a directory in the jar file called date_utils which contains the class ConvertDateFormat.