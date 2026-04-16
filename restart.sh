#!/bin/bash
toolforge jobs delete rustbot
\rm ~/rustbot.*
PORT=8089 toolforge jobs run --mem 3500Mi --cpu 3 --continuous --port 8089 \
	--mount=all \
	--image tool-mix-n-match/tool-mix-n-match:latest \
	--command "sh -c 'target/release/main server --config /data/project/mix-n-match/mixnmatch_rs/config.json'" \
	--filelog -o /data/project/mix-n-match/rustbot.out -e /data/project/mix-n-match/rustbot.err \
	rustbot
