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
  spawn ssh $login@$c "lsof -ti | xargs kill -9 2>/dev/null; rm -rf $remoteFolder;mkdir $remoteFolder"
  expect "password:"
  send "$password\r"
  expect eof

  spawn scp -r $localFolder$todeploy $login@$c:$remoteFolder
  expect "password:"
  send "$password\r"
  expect eof

  spawn ssh $login@$c "cd $remoteFolder/target; ls; nohup java -jar ./$nameOfTheJarToExecute > /dev/null 2>&1 & "
  expect "password:"
  send "$password\r"
  expect eof
}

wait