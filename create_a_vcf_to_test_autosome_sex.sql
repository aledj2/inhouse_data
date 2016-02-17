select chr,start,ID,ref,alt,100,"PASS","DP=100","GT","0/1", MAF
from inhouse_data.sex_chroms
where ref != '-' and alt !='-'
union
select chr,start,ID,upper(aled_ref),concat(upper(aled_ref),alt),100,"PASS","INDEL;DP=100","GT","0/1", MAF
from inhouse_data.sex_chroms
where ref = '-' 
union
select chr,start-2,ID,concat(upper(aled_ref),ref),upper(aled_ref),100,"PASS","INDEL;DP=100","GT","0/1", MAF
from inhouse_data.sex_chroms
where alt = '-'
union
select chr,start,ID,ref,alt,100,"PASS","DP=100","GT","0/1", MAF
from inhouse_data.autosomes
where ref != '-' and alt !='-'
union
select chr,start,ID,upper(aled_ref),concat(upper(aled_ref),alt),100,"PASS","INDEL;DP=100","GT","0/1", MAF
from inhouse_data.autosomes
where ref = '-' 
union
select chr,start-2,ID,concat(upper(aled_ref),ref),upper(aled_ref),100,"PASS","INDEL;DP=100","GT","0/1", MAF
from inhouse_data.autosomes
where alt = '-'