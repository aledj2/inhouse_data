'''
Created on 22 Oct 2015

@author: Aled
'''
import MySQLdb


class open_file():

    def __init__(self):
        # specify the file
        self.outputfile1 = "/home/aled/Documents/inhouse_data/hg19_inhouse_hom_count.txt"
        #self.outputfile1 = "I:\\inhouse_data\\hg19_het_inhouse_MAF.txt"
        #self.outputfile2 = "I:\\inhouse_data\\hg19_hom_inhouse_MAF.txt"

        #  define parameters used when connecting to database
        self.host = "127.0.0.1"
        self.port = int(3306)
        self.username = "root"
        self.passwd = "mysql"
        self.database = "inhouse_data"

    def run_query(self):
        outputfile = open(self.outputfile1, 'a')
        #outputfile2 = open(self.outputfile2, 'a')

        # sql statement to create a flat file containing hom count 
        sql1 = "select chr, start, stop, ref, alt, num_hom from sex_chroms where MAF is not null"

        sql2 = "select chr, start, stop, ref, alt, num_hom from autosomes where chr in (2,3,4,5,6,7,8,9) and comb_MAF is not null"

        sql3 = "select chr, start, stop, ref, alt, num_hom from autosomes where chr in (10,11,12,13,14,15,16,1) and comb_MAF is not null"

        sql4 = "select chr, start, stop, ref, alt, num_hom from autosomes where chr in (17,18,19,20,21,22) and comb_MAF is not null"

###############################################################################
#         # sql statement to create a flat file containing het count 
#         sql1 = "select chr, start, stop, ref, alt, num_het from sex_chroms where MAF is not null"
# 
#         sql2 = "select chr, start, stop, ref, alt, num_het from autosomes where chr in (2,3,4,5,6,7,8,9) and comb_MAF is not null"
# 
#         sql3 = "select chr, start, stop, ref, alt, num_het from autosomes where chr in (10,11,12,13,14,15,16,1) and comb_MAF is not null"
# 
#         sql4 = "select chr, start, stop, ref, alt, num_het from autosomes where chr in (17,18,19,20,21,22) and comb_MAF is not null"
###############################################################################

###############################################################################
#         # sql statement to create a flat file containing MAF 
#         sql1 = "select chr, start, stop, ref, alt, MAF from sex_chroms where MAF is not null"
# 
#         sql2 = "select chr, start, stop, ref, alt, comb_MAF from autosomes where chr in (2,3,4,5,6,7,8,9) and comb_MAF is not null"
# 
#         sql3 = "select chr, start, stop, ref, alt, comb_MAF from autosomes where chr in (10,11,12,13,14,15,16,1) and comb_MAF is not null"
# 
#         sql4 = "select chr, start, stop, ref, alt, comb_MAF from autosomes where chr in (17,18,19,20,21,22) and comb_MAF is not null"
###############################################################################

        # open connection to database and run SQL statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql1)
            flatfile1 = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read db"
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through the query result 
        for i in flatfile1:
            chr = i[0]
            start = i[1]
            stop = i[2]
            ref = i[3]
            alt = i[4]
            MAF = i[5]

            # print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
            outputfile.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
        flatfile1 = []
        # open connection to database and run SQL statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql2)
            flatfile2 = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read db "
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through the query result
        for i in flatfile2:
            chr = i[0]
            start = i[1]
            stop = i[2]
            ref = i[3]
            alt = i[4]
            MAF = i[5]

            # print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
            outputfile.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
        flatfile2 = []

        # open connection to database and run SQL statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql3)
            flatfile3 = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read db "
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through the query result
        for i in flatfile3:
            chr = i[0]
            start = i[1]
            stop = i[2]
            ref = i[3]
            alt = i[4]
            MAF = i[5]

            # print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
            outputfile.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
        flatfile3 = []

        # open connection to database and run SQL statement
        db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
        cursor = db.cursor()

        try:
            cursor.execute(sql4)
            flatfile4 = cursor.fetchall()
        except MySQLdb.Error, e:
            db.rollback()
            print "fail - unable to read db "
            if e[0] != '###':
                raise
        finally:
            db.close()

        # loop through the query result
        for i in flatfile4:
            chr = i[0]
            start = i[1]
            stop = i[2]
            ref = i[3]
            alt = i[4]
            MAF = i[5]

            # print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
            outputfile.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
        flatfile4 = []

################################################################################
# open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
#         db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
#         cursor = db.cursor()
#
#         try:
#             cursor.execute(sql5)
#             flatfile5 = cursor.fetchall()
#         except MySQLdb.Error, e:
#             db.rollback()
#             print "fail - unable to read db "
# if e[0] != '###':
#                 raise
#         finally:
#             db.close()
#
# loop through the query result adding the scores to the desired dictionary value
#         for i in flatfile5:
#             chr = i[0]
#             start = i[1]
#             stop = i[2]
#             ref = i[3]
#             alt = i[4]
#             MAF = i[5]
#
# print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
#             outputfile2.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
#         flatfile5 = []
# open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
#         db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
#         cursor = db.cursor()
#
#         try:
#             cursor.execute(sql6)
#             flatfile6 = cursor.fetchall()
#         except MySQLdb.Error, e:
#             db.rollback()
#             print "fail - unable to read db "
# if e[0] != '###':
#                 raise
#         finally:
#             db.close()
#
# loop through the query result adding the scores to the desired dictionary value
#         for i in flatfile6:
#             chr = i[0]
#             start = i[1]
#             stop = i[2]
#             ref = i[3]
#             alt = i[4]
#             MAF = i[5]
#
# print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
#             outputfile2.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
#         flatfile6 = []
#
# open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
#         db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
#         cursor = db.cursor()
#
#         try:
#             cursor.execute(sql7)
#             flatfile7 = cursor.fetchall()
#         except MySQLdb.Error, e:
#             db.rollback()
#             print "fail - unable to read db "
# if e[0] != '###':
#                 raise
#         finally:
#             db.close()
#
# loop through the query result adding the scores to the desired dictionary value
#         for i in flatfile7:
#             chr = i[0]
#             start = i[1]
#             stop = i[2]
#             ref = i[3]
#             alt = i[4]
#             MAF = i[5]
#
# print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
#             outputfile2.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
#         flatfile7 = []
#
# open connection to database and run SQL statement to extract the Z scores, the probe orderID and chromosome
#         db = MySQLdb.Connect(host=self.host, port=self.port, user=self.username, passwd=self.passwd, db=self.database)
#         cursor = db.cursor()
#
#         try:
#             cursor.execute(sql8)
#             flatfile8 = cursor.fetchall()
#         except MySQLdb.Error, e:
#             db.rollback()
#             print "fail - unable to read db "
# if e[0] != '###':
#                 raise
#         finally:
#             db.close()
#
# loop through the query result adding the scores to the desired dictionary value
#         for i in flatfile8:
#             chr = i[0]
#             start = i[1]
#             stop = i[2]
#             ref = i[3]
#             alt = i[4]
#             MAF = i[5]
#
# print chr + "\t" + start + "\t" + stop + "\t" + ref + "\t" + alt + "\t" + MAF
#             outputfile2.write(str(chr) + "\t" + str(start) + "\t" + str(stop) + "\t" + str(ref) + "\t" + str(alt) + "\t" + str(MAF) + "\n")
#         flatfile8 = []
################################################################################
        outputfile.close()
        # outputfile2.close()

if __name__ == '__main__':
    open_file().run_query()
    print "done"
