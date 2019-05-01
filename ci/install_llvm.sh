#!/bin/bash

function llvm_ubuntu {
  wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
  sudo apt-add-repository "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-6.0 main"
  sudo apt-get update
  sudo apt-get install llvm-6.0-dev clang-6.0

  sudo ln -s /usr/bin/llvm-config-6.0 /usr/local/bin/llvm-config

  sudo apt-get install -y zlib1g-dev
}

function llvm_darwin {
  brew install llvm@6
  sudo ln -s /usr/local/Cellar/llvm@6/6.0.1_1/bin/llvm-config /usr/local/bin/llvm-config
  echo 'export PATH="/usr/local/opt/llvm@6/bin:$PATH"' >> ~/.bash_profile
  export LDFLAGS="-L/usr/local/opt/llvm@6/lib"
  export CPPFLAGS="-I/usr/local/opt/llvm@6/include"
}

case "$(uname -s)" in
   Darwin)
     llvm_darwin
     ;;
   Linux)
     llvm_ubuntu
     ;;
   CYGWIN*|MINGW32*|MSYS*)
     echo 'unimplemented'
     ;;
   *)
     echo 'OS not supported'
     ;;
esac

