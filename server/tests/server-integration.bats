#!/usr/bin/env bats

@test "healthy" {
  curl -sSL -o tmp localhost:26379/
  cat tmp
  [ "$(cat tmp)" = "Healthy" ]
  curl -sSL -o tmp localhost:26380/
  cat tmp
  [ "$(cat tmp)" = "Healthy" ]
  curl -sSL -o tmp localhost:26381/
  cat tmp
  [ "$(cat tmp)" = "Healthy" ]
}

@test "get_file file correctness" {
  curl -sSL -o tmp localhost:26379/s3/test1.txt
  cat tmp
  [ "$(cat tmp)" = "$(cat $TEST_ROOT/test_s3_files/test1.txt)" ]
}