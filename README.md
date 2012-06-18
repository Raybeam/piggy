piggy
=====

<h1>Pig UDFS in Java</h1>

Follow the following steps for creating a jar file which can be used in your pig scripts:

<h2>Creating the jar file:</h2>

<ul>
<li>Download the jar libs from the lib directory</li>
<li>Copy the source and the jars in a folder called i.e. date_utils</li>
<li>Compile the code in date_utils javac -cp hadoop.jar:pig.jar *.java</li>
<li>Create the jar file by running jar -cf date_utils.jar date_utils/</li>
</ul>


<h3>You can define the function in pig like:</h3>

DEFINE ConvertDate date_utils.ConvertDateFormat();

date_utils.ConvertDateFormat() means that there is a directory in the jar file called date_utils which contains the class ConvertDateFormat.
