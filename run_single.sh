#!/bin/bash
toolforge jobs delete single

toolforge jobs run --wait --mem 2000Mi --cpu 1 --mount=all --image tool-mix-n-match/tool-mix-n-match:latest \
	-o /data/project/mix-n-match/mixnmatch_rs/single.out -e /data/project/mix-n-match/mixnmatch_rs/single.err \
	--command "sh -c 'target/release/main \"$1\" \"$2\" /data/project/mix-n-match/mixnmatch_rs/config.json'" single

toolforge jobs logs single
