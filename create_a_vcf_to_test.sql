select chr,start,ID,ref,alt,100,"PASS","DP=100","GT","0/1", MAF
from inhouse_data.sex_chroms
where ref != '-' and alt !='-' and `ID` in ('100' ,'1500','5000','19852','66553','99999','123456','238538','300138','13921','126766','169')
union
select chr,start,ID,upper(aled_ref),concat(upper(aled_ref),alt),100,"PASS","DP=100","GT","0/1", MAF
from inhouse_data.sex_chroms
where ref = '-' and `ID` in ('100' ,'1500','5000','19852','66553','99999','123456','238538','300138','13921','126766','169')
union
select chr,start-2,ID,concat(upper(aled_ref),ref),upper(aled_ref),100,"PASS","DP=100","GT","0/1", MAF
from inhouse_data.sex_chroms
where alt = '-' and `ID` in ('100' ,'1500','5000','19852','66553','99999','123456','238538','300138','13921','126766','169')