'''
Created on 12 Nov 2015

@author: aled
'''
import MySQLdb
import subprocess


class get_ref_allele():
    '''Extracts the insertions or deletions and runs this through a samtools program to find the reference allele at that position.
    Depending if its an ins or del either the ref allele (ins) or the two bases preceeding the del would be inserted to aled_ref column in db'''
    
    def __init__(self):

        # define parameters used when connecting to database
        self.host = "127.0.0.1"
        self.port = int(3306)
        self.username = "root"
        self.passwd = "mysql"
        self.database = "inhouse_data"

        # set parameters, ins or del and what table to update
        # self.ins_or_del = "del"
        self.ins_or_del = "ins"
        # self.autosome_or_sex = "autosomes"
        self.autosome_or_sex = "sex_chroms"

    def run_query(self):
        
        if self.ins_or_del == "del":
            # sql query to pull out all deletions in a table
            sql = """select chr,start,stop,concat("chr",chr,":",start,"-",stop),alt,ID  from """ + self.autosome_or_sex + """ where alt = '-' """

            # open connection to database and run SQL statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            try:
                cursor.execute(sql)
                missing_refs = cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to read db"
                if e[0] != '###':
                    raise
            finally:
                db.close()

            # loop through the query result
            for i in missing_refs:
                chrom = i[0]
                start = i[1]
                stop = i[2]
                ref = i[4]
                ID = i[5]

                # for deletions we need to find the two bases preceeding the deletion therefore adjust the start position to start - 2
                start_minus_2 = int(start) - 2
                # create the new coordinates
                del_coord = "chr" + chrom + ":" + str(start_minus_2) + "-" + str(stop)

                # run the samtools command via command line
                proc = subprocess.Popen(["samtools faidx /home/aled/Documents/Reference_Genomes/hg19.fa " + del_coord], stdout=subprocess.PIPE, shell=True)
                # capture stdout and stderror
                (out, err) = proc.communicate()

                # capture the reference base from the output
                ref_base = out[-(len(ref) + 3):-(len(ref) + 1)]
                if err is not None:
                    print err

                # sql
                sql2 = """update """ + self.autosome_or_sex + """ set Aled_ref = %s where ID=%s"""

                # open connection to database and run update statement
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()

                try:
                    cursor.execute(sql2, (str(ref_base), str(ID)))
                    db.commit()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read db"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()

        # For insertions
        elif self.ins_or_del == "ins":
            # sql query to pull out all insertions in a table
            sql = """select chr,start,stop,concat("chr",chr,":",start,"-",stop),ref,ID  from """ + self.autosome_or_sex + """ where ref = '-' """

            # open connection to database and run SQL statement
            db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
            cursor = db.cursor()

            try:
                cursor.execute(sql)
                missing_refs = cursor.fetchall()
            except MySQLdb.Error, e:
                db.rollback()
                print "fail - unable to read db"
                if e[0] != '###':
                    raise
            finally:
                db.close()

            # loop through the query result
            for i in missing_refs:
                coord = i[3]
                ID = i[5]

                # run the samtools command via command line using the coordinate from sql
                proc = subprocess.Popen(["samtools faidx /home/aled/Documents/Reference_Genomes/hg19.fa " + coord], stdout=subprocess.PIPE, shell=True)

                # capture stdout and stderror
                (out, err) = proc.communicate()
                if err is not None:
                    print err
                # capture the reference base from the output
                ref_base = out[-2:-1]

                # sql
                sql2 = """update """ + self.autosome_or_sex + " set Aled_ref = %s where ID=%s"""

                # open connection to database and run SQL statement to extract the
                db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
                cursor = db.cursor()

                try:
                    cursor.execute(sql2, (str(ref_base), str(ID)))
                    db.commit()
                except MySQLdb.Error, e:
                    db.rollback()
                    print "fail - unable to read db"
                    if e[0] != '###':
                        raise
                finally:
                    db.close()
        else:
            raise ValueError("unsure if ins or del")

if __name__ == '__main__':
    get_ref_allele().run_query()
    print "done"
