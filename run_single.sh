#!/bin/bash
toolforge jobs delete single

toolforge jobs run --wait --mem 2000Mi --cpu 1 --mount=all --image tool-mix-n-match/tool-mix-n-match:latest \
	--command "sh -c 'target/release/mixnmatch \"$1\" /data/project/mix-n-match/mixnmatch_rs/config.json' \"$2\"" single
#	-o /data/project/mix-n-match/mixnmatch_rs/single.out -e /data/project/mix-n-match/mixnmatch_rs/single.err \

toolforge jobs logs single
