#!/bin/bash
toolforge jobs delete single

toolforge jobs run --wait --mem 2000Mi --cpu 1 --mount=all --image tool-mix-n-match/tool-mix-n-match:latest \
	--command "php8.1 -c /data/project/mix-n-match/mixnmatch_rs/php.ini $1 $2 $3 $4" single
#	-o /data/project/mix-n-match/mixnmatch_rs/single.out -e /data/project/mix-n-match/mixnmatch_rs/single.err \

toolforge jobs logs single
