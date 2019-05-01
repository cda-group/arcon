#!/bin/bash

function linux_ubuntu {
  set -ex
  VERSION="3.6.1"
  NAME="protoc-$VERSION-linux-x86_64.zip"
  URL="https://github.com/protocolbuffers/protobuf/releases/download/v$VERSION/$NAME"

  # Make sure you grab the latest version
  wget "$URL"

  # Unzip
  unzip "$NAME" -d protoc3

  # Move protoc to /usr/local/bin/
  sudo mv protoc3/bin/* /usr/local/bin/

  # Move protoc3/include to /usr/local/include/
  sudo mv protoc3/include/* /usr/local/include/
}

function darwin {
  brew install protobuf@3.6
  sudo cp /usr/local/opt/protobuf@3.6/bin/protoc /usr/local/bin/
  sudo cp /usr/local/opt/protobuf@3.6/include/* /usr/local/include/
  echo 'export PATH="/usr/local/opt/protobuf@3.6/bin:$PATH"' >> ~/.bash_profile
  export LDFLAGS="-L/usr/local/opt/protobuf@3.6/lib"
  export CPPFLAGS="-I/usr/local/opt/protobuf@3.6/include"
}

case "$(uname -s)" in
   Darwin)
     darwin
     ;;
   Linux)
     linux_ubuntu
     ;;
   CYGWIN*|MINGW32*|MSYS*)
     echo 'unimplemented'
     ;;
   *)
     echo 'OS not supported'
     ;;
esac

