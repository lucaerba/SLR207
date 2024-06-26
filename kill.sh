#!/usr/bin/expect -f

set timeout -1
set login "lerbi-23"
set password "nk52KRc6s5K2@Kq"
set localFolder "./"
set todeploy "javaftp/myftpserver/target"
set remoteFolder "/dev/shm/$login/"
set nameOfTheJarToExecute "myftpserver-1-jar-with-dependencies.jar"
set computers [split [exec cat machines.txt] "\n"]

foreach c $computers {
  #kill all processes, remove remote folder and create it again, then deploy the jar 
  spawn ssh $login@$c "lsof -ti | xargs kill -9 2;"
  expect " password:"
  send "$password\r"
  expect eof


}

wait