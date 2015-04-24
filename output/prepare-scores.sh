#!/bin/bash

echo -e "prom\tpromRank\tpromDeltaTop\tpromDeltaSucc\tcontext\tcontextRank\tcontextDeltaTop\tcontextDeltaSucc\tclass" > header-scores
sleep 10
cut -f 5-13 scores.wiki > scores-cut
cat header-scores scores-cut > raw-scores.wiki
rm header-scores scores-cut
