#!/usr/bin/expect -f

set timeout -1
set login "lerbi-23"
set localFolder "./"
set todeploy "javaftp/myftpserver/target"
set remoteFolder "/dev/shm/$login/"
set nameOfTheJarToExecute "myftpserver-1-jar-with-dependencies.jar"
set computers [split [exec cat machines.txt] "\n"]

foreach c $computers {
  # SSH to the remote computer and prepare the remote folder
  spawn ssh $login@$c "lsof -ti | xargs kill -9 2>/dev/null; rm -rf $remoteFolder; mkdir -p $remoteFolder"
  expect eof

  # SCP the files to the remote computer
  spawn scp -r $localFolder$todeploy $login@$c:$remoteFolder
  expect eof

  # Construct the remote command to be run in the new terminal
  set remote_command "cd $remoteFolder/target && ls && java -jar ./$nameOfTheJarToExecute & tail -f /dev/null"

  # Debugging: Print the command to check correctness
  puts "ssh $login@$c \"$remote_command\""

  # Open a new macOS Terminal tab and execute the command
  exec osascript -e "tell application \"Terminal\" to do script \"ssh $login@$c \\\"$remote_command\\\"\""
}

wait
