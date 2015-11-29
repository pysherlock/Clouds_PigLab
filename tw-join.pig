
set default_parallel 20;
-- TODO: load the input dataset, located in ./local-input/OSN/tw.txt
dataset = LOAD './local-input/tw.txt' AS (id: int, fr: int);
dataset2 = LOAD './local-input/tw.txt' AS (id: int, fr: int);
-- TODO: compute all the two-hop paths 
twohop = JOIN dataset BY $1, dataset2 BY $0;

-- TODO: project the twohop relation such that in output you display only the start and end nodes of the two hop path
p_result = FOREACH twohop generate $0 AS id, $3 AS fr;
-- TODO: make sure you avoid loops (e.g., if user 12 and 13 follow eachother)

d_result = DISTINCT p_result;
result = FILTER d_result BY id != fr;

STORE result INTO './local-output/OSN/twj';

