#!/usr/bin/python

import urllib2
import urllib
import xml.etree.ElementTree as ET
from pprint import pprint
import argparse
import re
import os
import MySQLdb
import subprocess
import time
import tarfile
import sys
import smtplib
from email.mime.text import MIMEText


from copy import copy
from dateutil.parser import parse
import zipfile
from time import sleep

import logging
from logging.config import dictConfig

logging_config = dict(
        version = 1,
        formatters = {
            'f': {'format':
                      '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
        handlers = {
            'h': {'class': 'logging.StreamHandler',
                  'formatter': 'f',
                  'level': logging.DEBUG}
        },
        root = {
            'handlers': ['h'],
            'level': logging.DEBUG,
        },
)

dictConfig(logging_config)
logger = logging.getLogger()

#TODO GET FROM CONFIG
HOST = {'hostname': '10.35.240.32'}
CC = "NCLComProductionSupportTeam@ncl.com,ITSHIPAPPMOBILE@ncl.com,sky@ncl.com,ncl@Nettalk.com"



SECONDS_IN_A_DAY = 86400
DAYS_OLD = 3
REGEX = r"(?P<fileName>(?P<sailid>.*)_(?P<ship>.*)_(?P<year>.*)-(?P<month>.*)-(?P<day>[^\_]*)_[^\.]*\.sql.gz)"
PATTERN = re.compile(REGEX)

#TODO GET FROM CONFIG
DB_HOST = "127.0.0.1"
DB_USER = "app_export_user"
DB_PASSWORD = "d2aVMwhGXsXqtYsR6KLE76X6utDczEtkbB7!"
DB_PORT = 3306
DB = "ncl_export"

ADAPTER_URL = 'http://127.0.0.1:9001'
SHIP_SERVICE_URL = 'http://127.0.0.1:9002'
ns = {'ncl': 'http://nclapi/schemas'}


parser = argparse.ArgumentParser(description='Import the onboard export into the mysql database after the cruise has rolled over')

parser.add_argument('source',
                    metavar='<Source Folder>',
                    help='source/drop folder where files will be imported from')

parser.add_argument('backup',
                    metavar='<Backup Folder>',
                    help='backup folder where files will be backed up to')


args = parser.parse_args()

# -----------------------------------------
# Send completion email
# -----------------------------------------
def sendDoneEmail(shipSailId):
    try:
        # Open a plain text file for reading.  For this example, assume that
        # Create a text/plain message
        msg = MIMEText('Sky - Import complete...sailing %s.  Please test Cruise Norwegian App ' % shipSailId)

        # me == the sender's email address
        # you == the recipient's email address
        msg['Subject'] = 'Sky import complete with sailing %s ' % shipSailId
        msg['From'] = 'Sky Auto Importer'
        # msg['To'] = 'npersad@ncl.com'
        msg['Cc'] = CC
        rcpt = CC.split(",")

        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        s = smtplib.SMTP()
        s.connect('forwarder.sky.ncl.com')
        s.sendmail('npersad@ncl.com', rcpt, msg.as_string())
        s.quit()
    except:
        e = sys.exc_info()[0]
        logger.debug("Warning: %s was caught while trying to send your mail.\nContent:%s\n" % ("",e.message))
        pass

# -----------------------------------------
# Extract python module name
# -----------------------------------------
def extractModule(fname):
    _cwd = os.getcwd()
    tar = tarfile.open(fname)
    tar.extractall()
    tar.close()
    folder = os.path.splitext(os.path.splitext(os.path.basename(fname))[0])[0]
    # print('importing ' + cwd + '/' + folder)
    logger.debug('\timporting ' + _cwd + '/' + folder)
    sys.path.append(_cwd + '/' + folder)

# -----------------------------------------
# TODO get rid of this with log rolling eventually
# Clear Ship Services log
# -----------------------------------------
def clearShipServicesLog():
    command = """echo "" > /var/log/nclcom/AkkaShipServices.log"""
    logger.debug("clearing ship services log..." + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("cleared ship services log..." + result)


# -----------------------------------------
# Deactivate Nettalk
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def deactivateNettalk():
    command = """wget %s/cdr/deactivate""" % (SHIP_SERVICE_URL)
    logger.debug("Deactivating nettalk " + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("deactivating nettalk..." + result)

# -----------------------------------------
# Import nettalk Users
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def importNettalk():
    command = """wget %s/import/all""" % (SHIP_SERVICE_URL)
    logger.debug("Importing nettalk users " + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("Imported users..." + result)

# -----------------------------------------
# Charge nettalk users
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def chargeUsers():
    command = """wget %s/cdr/pay""" % (SHIP_SERVICE_URL)
    logger.debug("Charging users for calls..." + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("Done charging..." + result)

# -----------------------------------------
# Bounce Akka Ship Services
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def restartShipServices():
    command = """/etc/init.d/AkkaShipServices restart"""
    logger.debug("Restarting ship services" + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("Restarted ship services" + result)

# -----------------------------------------
# Bounce adapter
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def restartAdapter():
    command = """/etc/init.d/ncl-onboard-adapter restart"""
    logger.debug("Restarting NCL onboard adapter..." + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("Restarted adapter..." + result)

# -----------------------------------------
# Restart nclcom
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def restartNclcom():
    command = """/etc/init.d/nclcom restart"""
    logger.debug("Restarting nclcom ..." + command)
    result = subprocess.check_output(["ssh", HOST['hostname'], command])
    logger.debug("Restarted nclcom..." + result)

#this is ugly but I dont really have much options since the R4 ports arent open to the sql server
# -----------------------------------------
# Get the fidelio SysMsg
# -----------------------------------------
# TODO paramiko cant install on the mysql server because its missing redhat lib dependencies
# and NOT about to fix that in the middle of a sailing
def getSailing():
    command = """curl -s -X POST -H "Content-Type: text/xml" -d '<Onboard_CurrentCruise Version="3.1" Target="Production" EchoToken="String" PrimaryLangID="en-us" TransactionIdentifier="Reservation-160541-2017-10-16T10:31:49" AltLangID="en-us" SequenceNmbr="1" xmlns="http://nclapi/schemas" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"></Onboard_CurrentCruise>' http://127.0.0.1:9001"""
    result = subprocess.check_output(["ssh", "%s" % HOST['hostname'],command])

    logger.debug("SysMsgReq \n%s", result)
    root = ET.fromstring(result)
    return root

# -----------------------------------------
# Get the latest file in the drop directory
# -----------------------------------------
def getLatest():
    source = args.source

    os.chdir(source)

    a = [(os.path.getctime(f), f) for f in os.listdir(os.curdir)]
    a.sort()

    foundFiles = [fileName for fileName in [[m.groupdict() for m in PATTERN.finditer(fname[1])] for fname in a] if len(fileName) > 0 ]#[-1]

    logger.debug("latest file found is %s " % foundFiles[-1][0]['fileName'])

    return foundFiles[-1][0]

# -----------------------------------------
# Get the fidelio SysMsg
# -----------------------------------------
def getFidelioSailing():
    headers = {'content-type': 'application/xml', 'authorization': 'bmNsOjZlOTk5ZGViYWQyNTViNjE5N2Y0ZmI0NGE1ZTc1NzIzZTU1MGM5M2Q', 'accept' : 'application/xml'}
    body = """<Onboard_CurrentCruise Version="3.1" Target="Production" EchoToken="String" PrimaryLangID="en-us" TransactionIdentifier="Reservation-160541-2017-10-16T10:31:49" AltLangID="en-us" SequenceNmbr="1" xmlns="http://nclapi/schemas" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"></Onboard_CurrentCruise>"""


    req = urllib2.Request(url=ADAPTER_URL,
                          data=body,
                          headers=headers)
    res = urllib2.urlopen(req)
    response = res.read()

    # xRes =  xml.etree.ElementTree.fromstring(response)
    root = ET.fromstring(response)

    return root

# -----------------------------------------
# Import modules
# allows the script to be portable and not depend
# on the packages being installed on the host
# -----------------------------------------
def importModules():
    try:
        import paramiko
    except:
        print 'importing paramiko...'
        path = os.path.dirname(os.path.realpath(__file__)) + '/paramiko/'
        print 'base path ' + path
        for module in os.listdir(path):
            try:
                extractModule(path + '/' + module)
            except IOError, e:
                print '...'

        import paramiko

# -----------------------------------------
# Backup existing database
# -----------------------------------------
def dropDB():

    logger.debug("DROPPING DATABASE NCL_EXPORT")

    db = MySQLdb.connect(host=DB_HOST,    # your host, usually localhost
                         user=DB_USER,
                         passwd=DB_PASSWORD,
                         port=DB_PORT,
                         db=DB)
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    logger.debug("DROPPING DATABASE NCL_EXPORT")
    cursor.execute("drop database IF EXISTS ncl_export;")

    logger.debug("CREATING DATABASE NCL_EXPORT")

    cursor.execute("create database IF NOT EXISTS ncl_export;")

    db.commit()
    db.close()




    # backupArgs = ['/usr/bin/mysqladmin drop -f ' + DB]
    # logger.debug("dropping database ncl_export...%s" %backupArgs[0])
    # # dropRes = os.system('mysqladmin drop -f ncl_export')
    # result = subprocess.check_output(backupArgs, shell=True)
    #
    # # dropRes.wait()
    #
    #
    # createArgs = ['/usr/bin/mysqladmin create ' + DB]
    # logger.debug("creating database ncl_export...%s" %createArgs[0])
    #
    # result1 = subprocess.check_output(createArgs, shell=True)

    # createRes.wait()


# -----------------------------------------
# Backup existing database
# -----------------------------------------
def backupDB(shipSailId):
    logger.debug("BACKING UP database")
    filestamp = DB + '_' + shipSailId + '_' + time.strftime('%Y-%m-%d-%I_%M')

    FILENAME = "{0}{1}.sql.gz.bak".format(args.backup, filestamp)

    backupArgs = ['mysqldump', '--databases', DB]

    with open(FILENAME, 'wb', 0) as f:
        p1 = subprocess.Popen(backupArgs, stdout=subprocess.PIPE)
        p2 = subprocess.Popen('gzip', stdin=p1.stdout, stdout=f)
        p1.stdout.close()
        p2.wait()
        p1.wait()

# -----------------------------------------
# Import new database
# -----------------------------------------
def importDB(file):
    logger.debug("Importing database from file...{0}".format(file['fileName']))

    pv = ['pv', args.source + file['fileName']]
    gunzip = 'gunzip'
    importArgs = ['mysql', DB]

    p1 = subprocess.Popen(pv, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(gunzip, stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen(importArgs, stdin=p2.stdout)

    p1.stdout.close()
    p3.wait()
    p2.wait()
    p1.wait()

# ---------------------
# ------- START -------
# ---------------------
def main():
    # ..... fidelio sailing .....
    root = getSailing()

    shipCode = root.findall('ncl:SelectedSailing',ns)[0].find('ncl:ShipCode',ns).text
    start = root.findall('ncl:SelectedSailing',ns)[0].find('ncl:Start',ns).text
    end = root.findall('ncl:SelectedSailing',ns)[0].find('ncl:End',ns).text
    parsedStartDate = parse(start)
    sysDate = time.strptime("%s/%s/%s" %(parsedStartDate.day, parsedStartDate.month, parsedStartDate.year), "%d/%m/%Y")

    logger.debug("""Fidelio SysMsg...""")
    logger.debug("""\tSHIP %s"""% (shipCode))
    logger.debug("""\tYEAR %s""" % (parsedStartDate.year))
    logger.debug("""\tMONTH %s""" % ( parsedStartDate.month))
    logger.debug("""\tDAY %s""" % (parsedStartDate.day) )


    # ..... latest upload .....
    file = getLatest()
    fileShip = file['ship']
    fileYear = file['year']
    fileDay = file['day']
    fileSailId = file['sailid']
    fileName = file['fileName']
    fileDate = time.strptime("%s/%s/%s" %(file['day'], file['month'], file['year']), "%d/%m/%Y")

    logger.debug("""latest upload...""")
    logger.debug("""\tfileName %s """ % (file['fileName']))
    logger.debug("""\tship %s""" % ( file['ship']))
    logger.debug("""\tyear %s""" % ( file['year']))
    logger.debug("""\tmonth %s""" % (file['month']))
    logger.debug("""\tday %s """ % (file['day']))

    if not os.path.exists(args.backup):
        os.makedirs(args.backup)


    logger.debug(file['ship'].strip() )
    logger.debug(file['year'].strip() + ' = ' + str(parsedStartDate.year))
    logger.debug(file['month'].strip() + ' = ' + str(parsedStartDate.month))
    logger.debug(file['day'].strip() + ' = ' + str(parsedStartDate.day))


    #SysMsg must have similar parameters to file name pieces in order to import
    #Detect roll over by matching latest import parameters against SysMsg
    if (file['ship'] == shipCode and
                int(file['year'].strip()) == int(str(parsedStartDate.year)) and
                int(file['month'].strip()) == int(str(parsedStartDate.month)) and
                int(file['day'].strip()) == int(str(parsedStartDate.day)) and
                fileDate >= sysDate
        ):

        db = MySQLdb.connect(host=DB_HOST,    # your host, usually localhost
                             user=DB_USER,
                             passwd=DB_PASSWORD,
                             port=DB_PORT,
                             db=DB)        # name of the data base

        # you must create a Cursor object. It will let
        # you execute all the queries you need
        cur = db.cursor()

        # Use all the SQL you like
        cur.execute("SELECT SAIL_ID FROM ncl_onboard_user_reservations LIMIT 1")

        logger.debug("FETCHING ONBOARD SAIL ID")
        for row in cur.fetchall():
            shipSailId = str(row[0])
            if (shipSailId != file['sailid']):
                logger.info("NEW UPLOAD DETECTED...(%s != %s)" % (file['sailid'], shipSailId))

                # backup db
                backupDB(shipSailId)

                # deactivate all active nettalk users
                deactivateNettalk()

                # run call script one last time since its fed from the database
                # to log remaining calls that cant be charged
                # TODO get hard confirmation from akka service
                chargeUsers()
                # sleep(60)

                # backup db again
                backupDB(shipSailId)

                #drop the database
                logger.debug("DROPPING DATABASE NCL_EXPORT")
                cur.execute("drop database IF EXISTS ncl_export;")

                logger.debug("CREATING DATABASE NCL_EXPORT")
                cur.execute("create database IF NOT EXISTS ncl_export;")

                # import users
                importDB(file)

                # bounce r4
                restartAdapter()
                restartNclcom()
                clearShipServicesLog()
                restartShipServices()

                #warm up
                sleep(30)

                # run nettalk import script
                importNettalk()

                #send email done
                sendDoneEmail(file['sailid'])

            else:
                logger.info("Sailing has not rolled over...")

        db.close()

        logger.info("...goodbye!")

    else:
        logger.info("Sailing has not rolled over...")

main()


