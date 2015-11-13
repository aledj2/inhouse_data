# inhouse_data
These scripts interrogate a database which hold the in house variants used to filter commonly seen variants.

db2file creates a file which can be used by annovar to annotate with.

rescue_ref_alleles uses a samtools program to find the base at a certain position. This is required because the inhouse data has insertions in the form:
ref alt
-   T

but when annotating a vcf annovar requires:
ref alt
X  XT

annovar then converts this to the correct input of:
ref alt
-   T

This may seem unnessasary as the annovar input file could be created, skipping the conversion step but to fully test the data I wanted to replicate a vcf exactly how the vcf's are created by the pipeline:
ref alt
X   XT

For insertions this requred finding the reference base, placing this in the ref and also prepending it to the alt allele

For deletions it is more complex:
The in house data is in the form:
ref   alt
ACTG  -

However the vcfs produced by the pipeline are:
ref   alt
XXACTG  XX

Therefore we needed to find the bases immediately before the deleted segment, and capture these.

Once the database has been updated the sql statement can be used to create a vcf file.


