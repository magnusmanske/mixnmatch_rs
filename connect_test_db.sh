#!/bin/bash
ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
ssh magnus@login.toolforge.org -L 3317:termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud:3306 -N &
