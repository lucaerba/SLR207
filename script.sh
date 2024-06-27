#!/bin/bash

# Define directories and jar file names
client_dir="javaftp/myftpclient"
server_dir="javaftp/myftpserver"
client_jar="myftpclient-1-jar-with-dependencies.jar"
server_jar="myftpserver-1-jar-with-dependencies.jar"
server_list="server_list.txt"

# Function to build and copy JAR
build_and_copy_jar() {
    local dir=$1
    local jar_name=$2
    
    # Navigate to the directory
    cd $dir || exit
    
    # Run Maven clean and package
    mvn clean package || exit
    
    # Copy the JAR file to ../../
    cp "target/$jar_name" ../../ || exit
    
    # Return to the original directory
    cd - || exit
}

# Build and copy JAR for myftpclient
build_and_copy_jar $client_dir $client_jar

# Copy the server_list.txt file to the target directory for myftpclient
cp "$server_list" "$client_dir/target/" || exit

# Build and copy JAR for myftpserver
build_and_copy_jar $server_dir $server_jar

# Execute the deploy scripts
set timeout -1


