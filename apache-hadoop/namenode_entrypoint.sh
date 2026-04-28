#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Starting Hadoop WordCount job automation..."

# --- Java Source File Preparation ---
echo "Copying Java source file into the container..."
cp /src/WordCount.java /tmp/WordCount.java
echo "Java source file copied."

# --- HDFS Operations ---
echo "Performing HDFS operations..."
hdfs dfs -mkdir -p /input
echo "Uploading input files (.txt and .csv and .md) from /input-data to HDFS /input directory..."
# Using find and xargs for robust handling of multiple files with varying names.
# -maxdepth 1 ensures we only look in the top-level input-data directory.
# -type f ensures we only copy files.
# \( -name "*.txt" -o -name "*.csv" \) filters for text files OR csv files.
# -print0 and xargs -0 handle filenames with spaces or special characters.
# -I {} replaces {} with the found filename.
# hdfs dfs -put -f {} /input/ uploads the file and forces overwrite if it exists.
find /input-data -maxdepth 1 -type f \( -name "*.txt" -o -name "*.csv" -o -name "*.md" \) -print0 | xargs -0 -I {} hdfs dfs -put -f {} /input/
echo "Input .txt and .csv and .md files uploaded to HDFS /input directory."

# --- Java Compilation and JAR Creation ---
echo "Preparing for Java compilation and JAR creation..."
# Move Java file to /root for easier access and compilation
mv /tmp/WordCount.java /root/WordCount.java
echo "Moved WordCount.java to /root/"

# Verify current directory and change to /root/
echo "Current directory before compilation: $(pwd)"
cd /root/
echo "Changed directory to: $(pwd)"

# Set Hadoop classpath
echo "Exporting HADOOP_CLASSPATH..."
export HADOOP_CLASSPATH=$(hadoop classpath)
echo "HADOOP_CLASSPATH set."

# Compile the Java code
echo "Compiling WordCount.java..."
# Capture stderr and stdout for debugging. Redirect to a log file.
javac -encoding UTF-8 -classpath "$HADOOP_CLASSPATH" /root/WordCount.java > javac_output.log 2>&1
if [ $? -ne 0 ]; then
    echo "javac compilation failed. Check javac_output.log for details."
    cat javac_output.log
    exit 1
fi
echo "javac compilation successful."

# List generated files to verify .class files exist
echo "Listing generated files after compilation:"
ls -l
# Check if WordCount*.class files exist before creating the JAR
if ! ls WordCount*.class > /dev/null 2>&1; then
    echo "Error: No WordCount*.class files found after compilation. Cannot create JAR."
    exit 1
fi

# Create the JAR file
echo "Creating wordcount.jar..."
jar cf wordcount.jar WordCount*.class
echo "JAR file created."

# --- Run Hadoop Job ---
echo "Running Hadoop WordCount job..."
# Execute the WordCount job
# The job expects input from the directory specified by the first argument (args[0])
# and writes output to the directory specified by the second argument (args[1]).
# These are /input and /output respectively as per the original command.
hadoop jar wordcount.jar WordCount /input /output
echo "Hadoop WordCount job finished."

# --- Copy Output to Host ---
echo "Copying HDFS output to host directory..."
# Ensure the output directory in HDFS exists and has content before copying
if hdfs dfs -test -d /output && hdfs dfs -ls /output | grep -q 'part-r-'; then
    hdfs dfs -get /output/* /host_output/
    echo "HDFS output copied to /host_output/ on the host."
else
    echo "HDFS output directory /output is empty or does not exist. Skipping copy."
fi

# --- Keep container running for inspection ---
echo "Hadoop WordCount job completed successfully. Container will remain running."
tail -f /dev/null
