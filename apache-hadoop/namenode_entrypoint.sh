#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "========================================"
echo "Starting Hadoop WordCount job automation"
echo "========================================"

# --- Wait for data files from pipeline ---
echo "Waiting for input data files from pipeline..."
MAX_WAIT=600  # 10 min max
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    # Check if there are files in /input-data
    FILE_COUNT=$(find /input-data -maxdepth 1 -type f \( -name "*.txt" -o -name "*.csv" -o -name "*.md" \) | wc -l)
    if [ "$FILE_COUNT" -gt 0 ]; then
        echo "Found $FILE_COUNT data files in /input-data. Proceeding..."
        break
    fi
    echo "Waiting for data files... (${WAITED}s / ${MAX_WAIT}s)"
    sleep 5
    WAITED=$((WAITED + 5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "WARNING: Timeout waiting for data files. Will use whatever is available."
fi

# List what we have
echo "Input data files:"
ls -la /input-data/

# --- Java Source File Preparation ---
echo "Copying Java source file into the container..."
cp /src/WordCount.java /tmp/WordCount.java
echo "Java source file copied."

# --- HDFS Operations ---
echo "Performing HDFS operations..."
hdfs dfs -mkdir -p /input
echo "Uploading input files from /input-data to HDFS /input directory..."
find /input-data -maxdepth 1 -type f \( -name "*.txt" -o -name "*.csv" -o -name "*.md" \) -print0 | xargs -0 -I {} hdfs dfs -put -f {} /input/
echo "Input files uploaded to HDFS /input directory."

# List HDFS input
echo "Files in HDFS /input:"
hdfs dfs -ls /input

# --- Java Compilation and JAR Creation ---
echo "Preparing for Java compilation and JAR creation..."
mv /tmp/WordCount.java /root/WordCount.java
echo "Moved WordCount.java to /root/"

cd /root/
echo "Changed directory to: $(pwd)"

echo "Exporting HADOOP_CLASSPATH..."
export HADOOP_CLASSPATH=$(hadoop classpath)
echo "HADOOP_CLASSPATH set."

echo "Compiling WordCount.java..."
javac -encoding UTF-8 -classpath "$HADOOP_CLASSPATH" /root/WordCount.java > javac_output.log 2>&1
if [ $? -ne 0 ]; then
    echo "javac compilation failed. Check javac_output.log for details."
    cat javac_output.log
    exit 1
fi
echo "javac compilation successful."

echo "Listing generated files after compilation:"
ls -l
if ! ls WordCount*.class > /dev/null 2>&1; then
    echo "Error: No WordCount*.class files found after compilation. Cannot create JAR."
    exit 1
fi

echo "Creating wordcount.jar..."
jar cf wordcount.jar WordCount*.class
echo "JAR file created."

# --- Run Hadoop Job ---
echo "Running Hadoop WordCount job..."
hadoop jar wordcount.jar WordCount /input /output
echo "Hadoop WordCount job finished."

# --- Copy Output to Host ---
echo "Copying HDFS output to host directory..."
if hdfs dfs -test -d /output && hdfs dfs -ls /output | grep -q 'part-r-'; then
    hdfs dfs -get /output/* /host_output/
    echo "HDFS output copied to /host_output/ on the host."
    echo "Files in /host_output/:"
    ls -la /host_output/
else
    echo "HDFS output directory /output is empty or does not exist. Skipping copy."
fi

# --- Signal completion (the pipeline generates the JSON from part-r-* files) ---
echo "Writing completion signal..."
echo "HADOOP_WORDCOUNT_COMPLETED" > /host_output/.hadoop_complete
date > /host_output/.hadoop_completed_at

# --- Keep container running for inspection ---
echo "========================================"
echo "Hadoop WordCount job completed successfully"
echo "Container will remain running for inspection."
echo "========================================"
tail -f /dev/null
