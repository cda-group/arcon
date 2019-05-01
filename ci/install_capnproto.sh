#!/bin/sh

case "$(uname -s)" in
   Darwin)
     brew install capnp
     ;;
   Linux)
     sudo apt update
     sudo apt install -y capnproto
     ;;
   CYGWIN*|MINGW32*|MSYS*)
     echo 'unimplemented'
     ;;
   *)
     echo 'OS not supported'
     ;;
esac
