'''
Created on 22 Oct 2015

@author: Aled
'''
import MySQLdb


class open_file():

    def __init__(self):
        # specify the file
        self.outputfile1 = "/home/aled/Documents/inhouse_data/all_inhouse.vcf"
        # self.outputfile1 = "I:\\inhouse_data\\hg19_het_inhouse_MAF.txt"
        # self.outputfile2 = "I:\\inhouse_data\\hg19_hom_inhouse_MAF.txt"

        #  define parameters used when connecting to database
        self.host = "127.0.0.1"
        self.port = int(3306)
        self.username = "root"
        self.passwd = "mysql"
        self.database = "inhouse_data"

    def run_query(self):
        outputfile = open(self.outputfile1, 'a')
        
        # sql statement  
        
        sql = "select chr,start,ID,ref,alt,100,'PASS','DP=100','GT','0/1', MAF \
        from inhouse_data.sex_chroms \
        where ref != '-' and alt !='-' and MAF is not Null\
        union \
        select chr,start,ID,upper(aled_ref),concat(upper(aled_ref),alt),100,'PASS','INDEL;DP=100','GT','0/1', MAF \
        from inhouse_data.sex_chroms \
        where ref = '-' and MAF is not Null\
        union \
        select chr,start-2,ID,concat(upper(aled_ref),ref),upper(aled_ref),100,'PASS','INDEL;DP=100','GT','0/1', MAF \
        from inhouse_data.sex_chroms \
        where alt = '-' and MAF is not Null "
        
        ########################################################################
        # sql = "select chr,start,ID,ref,alt,100,'PASS','DP=100','GT','0/1', MAF \
        # from inhouse_data.autosomes import \
        # where ref != '-' and alt !='-' and chr = %s and MAF is not Null\
        # union\
        # select chr,start,ID,upper(aled_ref),concat(upper(aled_ref),alt),100,'PASS','INDEL;DP=100','GT','0/1', MAF\
        # from inhouse_data.autosomes\
        # where ref = '-' and chr = %s and MAF is not Null\
        # union\
        # select chr,start-2,ID,concat(upper(aled_ref),ref),upper(aled_ref),100,'PASS','INDEL;DP=100','GT','0/1', MAF\
        # from inhouse_data.autosomes\
        # where alt = '-' and chr = %s and MAF is not Null"  
        ########################################################################
        
        
        # for i in range(1,23):
        # open connection to database and run SQL statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read db"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through the query result 
        for i in result:
            chr = str(i[0])
            start = str(i[1])
            ID = str(i[2])
            ref = str(i[3])
            alt = str(i[4])
            MAF = str(i[5])
            QUAL = str(i[6])
            FILTER = str(i[7])
            INFO = str(i[8])
            FORMAT = str(i[9])
            SAMPLE = str(i[10])
            
            tab = "\t"
            
            outputfile.write(chr + tab + start + tab + ID + tab + ref + tab + alt + tab + MAF + tab + QUAL + tab + FILTER + tab + INFO + tab + FORMAT + tab + SAMPLE + "\n")
        
        outputfile.close()
        

if __name__ == '__main__':
    open_file().run_query()
    print "done"
