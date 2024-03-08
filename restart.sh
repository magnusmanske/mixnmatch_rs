#!/bin/bash
toolforge jobs delete rustbot
toolforge jobs run --mem 5000Mi --cpu 3 --continuous --mount=all \
	--image tool-.mix-n-match/tool-.mix-n-match:latest \
	--command "sh -c 'target/release/main server /data/project/mix-n-match/mixnmatch_rs/config.json'" \
	rustbot
