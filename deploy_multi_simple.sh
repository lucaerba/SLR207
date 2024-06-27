#!/usr/bin/expect -f

set timeout -1
set login "lerbi-23"
set localFolder "./"
set todeploy "Simple.py"
set remoteFolder "/dev/shm/$login/"
set computers [split [exec cat machine_simple.txt] "\n"]


foreach c $computers {
  # SSH to the remote computer and prepare the remote folder
  spawn ssh $login@$c "lsof -ti | xargs kill -9 2>/dev/null; rm -rf $remoteFolder; mkdir -p $remoteFolder"
  expect eof

  # SCP the files to the remote computer
  spawn scp $localFolder$todeploy $login@$c:$remoteFolder
  expect eof

  # Construct the remote command to be run in the new terminal
  set remote_command "cd $remoteFolder && python3 $todeploy"

  # Debugging: Print the command to check correctness
  puts "ssh $login@$c \"$remote_command\""

  # Open a new macOS Terminal tab and execute the command
  exec osascript -e "tell application \"Terminal\" to do script \"ssh $login@$c \\\"$remote_command\\\"\""
}

wait
