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
            <li>Serves as the backbone for data processing. It's used for both extracting data from the source and performing complex data transformations.</li>
            <li>Spark's in-memory processing capabilities ensure efficient handling of large datasets.</li>
        </ul>
    </li>

<li>
    <strong>Apache Kafka 2.8.2:</strong>
    <ul>
        <li>Facilitates real-time data streaming.</li>
        <li>Acts as a central hub for data flows, ensuring a smooth and reliable transfer of data from the Spark producer to the Spark consumer.</li>
    </ul>
</li>

<li>
    <strong>MySQL:</strong>
    <ul>
        <li>Used as the data storage system.</li>
        <li>Hosts the transformed data and the additional tables created for analysis.</li>
    </ul>
</li>

<li>
    <strong>Python Libraries (PySpark, PyMySQL, Pandas, Matplotlib):</strong>
    <ul>
        <li>PySpark is used for interfacing with Apache Spark through Python.</li>
        <li>PyMySQL allows for interaction with the MySQL database.</li>
        <li>Pandas provide additional data manipulation capabilities.</li>
        <li>Matplotlib is used for creating basic visualizations of the processed data.</li>
    </ul>
</li>

<li>
    <strong>Microsoft Power BI:</strong>
 <ul>
        <li>Employed for more advanced data visualizations.</li>
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
            <li>Data is then structured according to a pre-defined schema, and initial transformations are applied (e.g., handling and filling null values).</li>
        </ul>
    </li>

<li>
    <strong>Data Streaming (Apache Kafka):</strong>
    <ul>
        <li>The transformed data is converted to JSON format.</li>
        <li>Apache Kafka is then used to stream the data, facilitating real-time data processing.</li>
    </ul>
</li>

<li>
    <strong>Further Transformation and Loading (Apache Spark & MySQL):</strong>
    <ul>
        <li>A PySpark consumer processes the streamed data, applying further transformations.</li>
        <li>The final transformed data is loaded into a MySQL database.</li>
        <li>Additional tables are created within the database to facilitate analysis.</li>
    </ul>
</li>

<li>
    <strong>Data Analysis and Visualization (Matplotlib & Microsoft Power BI):</strong>
    <ul>
        <li>Data is retrieved from the MySQL database for analysis.</li>
        <li>Simple analytical queries are executed to determine the top 10 airplane types, locations, and years with the highest frequency of crashes.</li>
        <li>Results are visualized using both Matplotlib for basic charts and Microsoft Power BI for more intricate and interactive visualizations.</li>
    </ul>
</li>
</ol>
<h3>To-Do:</h3>
<ul>
    
Refactor the code to be more optimized

Add Data Quality Checks

Add Logging for each processed patch of data

Enhance the Data Visualization to create a more interactive, in depth and dynamic dashboard

</ul>

![image](https://github.com/AhmedMetwaly1287/AirplaneCrashesETL/assets/139663311/7b2b56ea-68e8-4816-89f9-55dc103d778c)



