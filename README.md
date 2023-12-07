# BDS-Programming-Assignment (2023 - 2024) - Streaming Processing

## How to setup the environment
1. Install JDK **11.0.21**
- Download here: https://www.oracle.com/in/java/technologies/downloads/#java11-windows
- We are using this very old version of java, because Flink has [compatibility issue](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/java_compatibility/)

2. Install Maven **3.9.5**
- Download `apache-maven-3.9.5-bin.zip` here: https://maven.apache.org/download.cgi
- Follow the installation instruction: https://maven.apache.org/install.html

3. Create a new Flink (**1.17.1**) application using Maven
- Open a command prompt
- Run the following command ([reference](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/configuration/overview/)): `mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.17.1 -DgroupId=bds -DartifactId=assignment2 -Dversion=2023 -Dpackage=bds -DinteractiveMode=false`
- Enter the folder `assignment2`
- Change the `target.java.version` in `pom.xml` file to `11` (use any editor)

4. Install Eclipse
- Download here: https://www.eclipse.org/downloads/packages/installer
- Select `Eclipse IDE for Java Developers`

5. Import the Flink project to Eclipse
- Import projects --> Maven --> Existing Maven Projects --> select folder `assignment2` --> Finish
- Open a command prompt, enter the `assignment2` folder, run `mvn eclipse:eclipse`
- In Eclipse, refresh the project, then you'll see `JRE System Library` and `Referenced Libraries`
- Make sure it is `jdk-11` for the `JRE System Library`, if it's not, do the following
  - Right click `JRE System Library` --> Properties --> Alternate JRE --> Installed JREs --> select jdk-11 (if it does not appear here, click Add --> Standard VM --> Next --> choose Directory for the JRE home, it is the place where the jdk is installed, e.g. `C:\Program Files\Java\jdk-11`) --> Apply and Close
- Try to run `DataStreamJob.java` as a java application, you'll get error 
  > No operators defined in streaming topology. Cannot execute.
- Add more dependencies to `pom.xml`
  - add `flink-connector-files` to be able to read stream data from files
  - add `flink-connector-kafka` to be able to read stream data from kafka channels (https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/configuration/maven/)
  - add `msgpack-core` to be able to de-serialize data serialized by the Orleans application (https://mvnrepository.com/artifact/org.msgpack/msgpack-core)

    ```
    <dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-files</artifactId>
		  <version>${flink.version}</version>
		</dependency>
		
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-kafka</artifactId>
			<version>${flink.version}</version>
		</dependency>
		
		<dependency>
			<groupId>org.msgpack</groupId>
			<artifactId>msgpack-core</artifactId>
			<version>0.9.6</version>
		</dependency>
    ```
6. Load assignment code to the flink sample project
- Create a new package `EventManager` in `assignment2\src\main\java\`
- Import the 6 `.java` files from the folder `BDS-Programming-Assignment-2\FlinkWorld\code\EventManager` to this package
  - Right click `EventManager` package --> Import --> General --> File System --> Next --> select the `BDS-Programming-Assignment-2\FlinkWorld\code\EventManager\` folder --> finish
- Create a new package `WatermarkManager` in `assignment2\src\main\java\`
- Import the 2 `.java` files from the folder `BDS-Programming-Assignment-2\FlinkWorld\code\WatermarkManager` to this package
- Copy paste all the code from `BDS-Programming-Assignment-2\FlinkWorld\code\DataStreamJob.java` to `assignment2\src\main\java\bds\DataStreamJob.java`

7. Change the file path in line 52 of `DataStreamJob.java` to the path of the folder `BDS-Programming-Assignment-2\Data\`.

8. Disable the Flink logging console print
- Go to the file `assignment2\src\main\resources\log4j2.properties`
- Comment out the two lines
  - `rootLogger.level = INFO`
  - `rootLogger.appenderRef.console.ref = ConsoleAppender`
- Add one line `rootLogger = OFF`

9. Another option: import the prepared Flink assignment project directly
- Do step 1 and 2
- Open Eclipse --> Import projects --> Maven --> Existing Maven Projects --> select folder `BDS-Programming-Assignment-2\FlinkWorld\assignment2` --> Finish

10. Now you are ready for Programming Assignment 2 Part 1.