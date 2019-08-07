#!/bin/sh

case "$(uname -s)" in
   Darwin)
     brew install scala@2.12
     brew install sbt@1
     ;;
   Linux)
    echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    sudo apt-get update
    sudo apt-get install sbt
     ;;
   CYGWIN*|MINGW32*|MSYS*)
     echo 'unimplemented'
     ;;
   *)
     echo 'OS not supported'
     ;;
esac
