#!/bin/bash
toolforge jobs delete rustbot
\rm ~/rustbot.*
toolforge jobs run --mem 4000Mi --cpu 3 --continuous --mount=all \
	--image tool-mix-n-match/tool-mix-n-match:latest \
	--command "sh -c 'target/release/mixnmatch server /data/project/mix-n-match/mixnmatch_rs/config.json'" \
	--filelog -o /data/project/mix-n-match/rustbot.out -e /data/project/mix-n-match/rustbot.err \
	rustbot
