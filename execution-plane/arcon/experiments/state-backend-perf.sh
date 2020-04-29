#!/usr/bin/env bash
cd "${0%/*}" || exit 1
mkdir target

for backend in {rocks,sled,faster}; do
  echo -n "Warming up ${backend}... "
  for i in $(seq 1 5); do
    echo -n "${i} "
    # shellcheck disable=SC2068
    cargo run --release $@ --bin perf -- --state-backend-type metered${backend} > /dev/null 2>&1
  done
  echo "done."

  echo -n "Running ${backend}... "
  # shellcheck disable=SC2068
  cargo run --release $@ --bin perf -- --state-backend-type metered${backend} > ./target/${backend}.log 2> /dev/null
  echo "done."
done