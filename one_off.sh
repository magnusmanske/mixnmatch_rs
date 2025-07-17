#!/bin/bash
jobname=$1
root=/data/project/mix-n-match/mixnmatch_rs
out_file="${root}/jobstatus/${jobname}.out"
err_file="${root}/jobstatus/${jobname}.err"
command="target/release/main $@ --config ${root}/config.json"
rm ${out_file}
rm ${err_file}
toolforge jobs run --mem 200Mi --mount=all \
	--image tool-mix-n-match/tool-mix-n-match:latest \
	--command "${command}" \
	--filelog -o ${out_file} -e ${err_file} \
	${jobname}
