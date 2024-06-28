#!/usr/bin/expect -f

set timeout -1
set login "lerbi-23"
set localFolder "./"
set todeploy "WordCountProcessor.java"
set remoteFolder "/dev/shm/$login/"
set computers [split [exec cat machine_simple.txt] "\n"]

foreach c $computers {
  # SSH to the remote computer and prepare the remote folder
  spawn ssh $login@$c "lsof -ti | xargs kill -9 2>/dev/null; rm -rf $remoteFolder; mkdir -p $remoteFolder"
  expect eof

  # SCP the Java source file to the remote computer
  spawn scp $localFolder$todeploy $login@$c:$remoteFolder
  expect eof

  # Construct the remote command to compile and run the Java program
  set remote_command "cd $remoteFolder && javac $todeploy && java WordCountProcessor"

  # Debugging: Print the command to check correctness
  puts "ssh $login@$c \"$remote_command\""

  # Open a new Windows Terminal tab and execute the command
  exec wt.exe new-tab --title "Java Execution on $c" -- wsl bash -c "ssh $login@$c \\\"$remote_command\\\""
}

wait
