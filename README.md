<h1>Airplane Crashes ETL Pipeline & Analysis</h1>

<h2>Overview</h2>

<p><strong>A Simple Extract, Transform, Load Data Pipeline</strong> - This project is designed as a practical application of Big Data tools to demonstrate basic knowledge in building an ETL pipeline and Big Data Tools.</p>

<h2>Project Details</h2>

<p>The project demonstrates a workflow that integrates various technologies and libraries, Below is a detailed breakdown of the tools and their specific roles in the pipeline:</p>

<h3>Tools Used</h3>

<ol>
    <li>
        <strong>Apache Spark 3.5.0:</strong>
        <ul>
            <li>Used to carry out Data Transformations, Loading the data into the dataset</li>
        </ul>
    </li>

<li>
    <strong>MySQL:</strong>
    <ul>
        <li>Used as the data storage system.</li>
        <li>Hosts the transformed data created for analysis.</li>
    </ul>
</li>

<li>
    <strong>Python Libraries (PySpark, PyMySQL):</strong>
    <ul>
        <li>PySpark is used for interfacing with Apache Spark through Python.</li>
        <li>PyMySQL allows for interaction with the MySQL database.</li>
    </ul>
</li>

<li>
    <strong>Microsoft Power BI:</strong>
 <ul>
        <li>Employed for advanced data visualizations.</li>
        <li>Facilitates deeper insights by creating interactive reports and dashboards.</li>
</ul>
</li>
</ol>

<h3>Pipeline Workflow</h3>

<ol>
    <li>
        <strong>Data Extraction and Initial Transformation (Apache Spark):</strong>
        <ul>
            <li>A PySpark-based producer extracts data from CSV files.</li>
            <li>Data is then structured according to a pre-defined schema inside the CSV file, and initial transformations are applied (e.g., handling null values).</li>
        </ul>
    </li>


<li>
    <strong>Further Transformation and Loading (Apache Spark & MySQL):</strong>
    <ul>
        <li>A PySpark consumer processes the streamed data.</li>
        <li>The final transformed data is loaded into a MySQL database.</li>
    </ul>
</li>

<li>
    <strong>Exploratory Analysis and Visualization (Microsoft Power BI):</strong>
    <ul>
        <li>Data is retrieved from the MySQL database for analysis.</li>
        <li>Simple analytical queries are executed to determine the top 10 airplane types, locations, and years with the highest frequency of crashes.</li>
        <li>Results are visualized using Microsoft Power BI for more intricate and interactive visualizations.</li>
    </ul>
</li>
</ol>
<h3>To-Do:</h3>
<ul>
    
Enhance the Data Visualization to create a more interactive, in depth and dynamic dashboard

</ul>

![image](https://github.com/AhmedMetwaly1287/AirplaneCrashesETL/assets/139663311/7b2b56ea-68e8-4816-89f9-55dc103d778c)



