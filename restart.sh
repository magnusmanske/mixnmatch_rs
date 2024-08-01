#!/bin/bash
toolforge jobs delete rustbot
mv ~/rustbot.out ~/rustbot.out.bak
toolforge jobs run --mem 5000Mi --cpu 3 --continuous --mount=all \
	--image tool-mix-n-match/tool-mix-n-match:latest \
	--command "sh -c 'target/release/mixnmatch server /data/project/mix-n-match/mixnmatch_rs/config.json'" \
	rustbot
