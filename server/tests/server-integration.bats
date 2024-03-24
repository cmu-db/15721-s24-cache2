#!/usr/bin/env bats

@test "get_file file correctness" {
  result="$(curl -L localhost:26739/s3/test1.txt)"
  [ "$result" -eq  $(cat test_s3_files/test1.txt)]
}