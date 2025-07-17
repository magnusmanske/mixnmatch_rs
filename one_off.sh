#!/bin/bash
jobname=$1
@rm /data/project/mix-n-match/${jobname}.out
@rm /data/project/mix-n-match/${jobname}.err
toolforge jobs run --mem 200Mi --mount=all \
	--image tool-mix-n-match/tool-mix-n-match:latest \
	--command "sh -c 'target/release/main $@ --config /data/project/mix-n-match/mixnmatch_rs/config.json'" \
	--filelog -o /data/project/mix-n-match/${jobname}.out -e /data/project/mix-n-match/${jobname}.err \
	${jobname}
