piggy
=====

<h1>Pig UDFS in Java</h1>

Follow the following steps for creating a jar file which can be used in your pig scripts:

<h2>Creating the jar file:</h2>

<ul>
<li>Download the jar libs from the lib directory</li>
<li>Copy the source and the jars in a folder called i.e. date_utils</li>
<li>Compile the code in date_utils javac -cp hadoop-core-1.0.2.jar:date_utils.jar:jedis-2.1.0.jar:pig-0.9.2.jar:json-lib-2.4-jdk15.jar:commons-lang-2.6.jar:commons-collections-3.2.1.jar:io_utils.jar:UserAgentUtils-1.6.jar *.java</li>
<li>Create the jar file by running jar -cf date_utils.jar date_utils/</li>
</ul>


<h3>You can define the function in pig like:</h3>

DEFINE ConvertDate date_utils.ConvertDateFormat();

date_utils.ConvertDateFormat() means that there is a directory in the jar file called date_utils which contains the class ConvertDateFormat.
