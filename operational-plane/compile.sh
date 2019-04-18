#!/bin/bash

RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
BLUE=$(tput setaf 4)
WHITE=$(tput setaf 7)
RESET=$(tput sgr0)

SCALA_VER="2.12"

mkdir -p build

function cpy_jars {
    case "$1" in
    "sm")
        cp statemaster/target/scala-"$SCALA_VER"/statemaster.jar build/statemaster.jar
        ;;
    "am")
        cp appmaster/target/scala-"$SCALA_VER"/appmaster.jar build/appmaster.jar
        ;;
    "coordinator")
        cp coordinator/target/scala-"$SCALA_VER"/coordinator.jar build/coordinator.jar
        ;;
    "all")
        cp statemaster/target/scala-"$SCALA_VER"/statemaster.jar build/statemaster.jar
        cp coordinator/target/scala-"$SCALA_VER"/coordinator.jar build/coordinator.jar
        cp appmaster/target/scala-"$SCALA_VER"/appmaster.jar build/appmaster.jar
        ;;
    esac
}


case "$1" in
    "statemaster"|"sm")
        echo $BLUE"Compiling Statemaster..."
        sbt_fail=0
        (sbt statemaster/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile Statemaster"
            exit 1
        else
            echo $BLUE"statemaster.jar is being moved to build/"
            cpy_jars "sm"
        fi
        ;;  
    "appmaster"|"am")
        echo $BLUE"Compiling Appmaster..."
        sbt_fail=0
        (sbt appmaster/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile Appmaster"
            exit 1
        else
            echo $BLUE"appmaster.jar is being moved to build/"
            cpy_jars "am"
        fi
        ;;  
    "coordinator"|"cr")
        echo $BLUE"Compiling Coordinator..."
        sbt_fail=0
        (sbt coordinator/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile Coordinator"
            exit 1
        else
            echo $BLUE"coordinator.jar is being moved to build/"
            cpy_jars "coordinator"
        fi
        ;;  
    *)
        echo $WHITE"Compiling everything..."
        sbt_fail=0
        # Would be great to do this within a single sbt instance
        (sbt statemaster/assembly); (sbt coordinator/assembly); (sbt appmaster/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile"
            exit 1
        else
            echo $WHITE"Moving statemaster, appmaster, and coordinator to build/"
            cpy_jars "all"
        fi
        ;;
esac
