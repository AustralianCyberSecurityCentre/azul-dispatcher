#/bin/bash
# Run benchmarking for hashing

go test ./streams/identify -bench=${1:-.} -run=xx -p 10 -benchtime=1x


# ALL HASHES
# TINY   0.2162seconds
# medium 25.3seconds

# NO TLSH OR SSDEEP
# TINY   0.021seconds
# medium 2.432seconds

# NO SSDEEP
# TINY   0.1695seconds
# medium 19.97seconds

# NO TLSH
# TINY   0.071seconds  (0.69)
# medium 8.04seconds   (7.52)


# WITH SSDEEP AND TLSH, alternative golang implementation (github.com/glaslos/ssdeep)
# TINY 0.2288
# MEDIUM 25.80

# NO TLSH with alternative SSDEEP implementation
# TINY   0.093       (0.94)
# MEDIUM 10.34       (9.44)