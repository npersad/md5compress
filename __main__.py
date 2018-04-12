#!/usr/bin/python

#TODO ZIPINFO MODULE does not read more than 65k files

import fileinput
import re
from pprint import pprint
import sys
from filecmp import dircmp
import hashlib
import zipfile
import datetime
import zipfile
import json
from shutil import copyfile
import pkgutil
import os
import tarfile
import shutil

zipName = os.path.dirname(os.path.realpath(__file__))

# print("----------- <MANIFEST> ----------- ")
manifest = pkgutil.get_data('_files', 'manifest.json')
# with open(data, 'r') as data_file:
data = json.loads(manifest)

softlinks = json.loads(pkgutil.get_data('_files', 'symLinks.json'))
deletions = json.loads(pkgutil.get_data('_files', 'deletions.json'))

sourceDirectory = pkgutil.get_data('_files', 'source')
sourceDirectory = sourceDirectory.split('/')
sourceDirectory = "/".join(sourceDirectory[1:-1]).join(("/", ""))
startSourceRealtive = pkgutil.get_data('_files', 'source').split('/')[-1]
sourceRegex = re.escape(startSourceRealtive) + r"(.*)"  # r"startReltive(.*)"

print("\n\n----------- <TARGET> ----------- ")
targetDirectory = pkgutil.get_data('_files', 'target')
targetDirectory = targetDirectory.split('/')
targetDirectory = "/".join(targetDirectory[1:-1]).join(("/", ""))
startRealtive = pkgutil.get_data('_files', 'target').split('/')[-1]
destinationRegex = re.escape(startRealtive) + r"(.*)"  # r"startReltive(.*)"
print targetDirectory
print destinationRegex
print startRealtive

# TODO make variable or get from the archive through listing contents
# TODO the zipimport module in python wont recognize an archive with more than 65300 + files so we need to fully extract for now
# fh = open('zipfile_write.zip', 'rb')
# z = zipfile.ZipFile(fh)
# for name in z.namelist():
#     print 'from name list %s' %(name)
#     z.extract(name, outpath)
# fh.close()


def convertLocation(loc):
    relativeFilePathMatches = re.search(destinationRegex, loc, re.UNICODE)
    destinationFolder = os.path.dirname(targetDirectory + "/" + startRealtive + relativeFilePathMatches.group(1))
    destinationName = os.path.basename(loc)
    link = destinationFolder + "/" + destinationName
    return link

def convertSourceLocation(loc):
    relativeFilePathMatches = re.search(sourceRegex, loc, re.UNICODE)
    destinationFolder = os.path.dirname(targetDirectory + "/" + startRealtive + relativeFilePathMatches.group(1))
    destinationName = os.path.basename(loc)
    link = destinationFolder + "/" + destinationName
    return link

def importModuleFromZip(at, zip, fname):
    import sys
    os.chdir("/tmp")
    cwd = os.getcwd()

    zip.extract( fname, cwd)
    tar = tarfile.open(cwd + '/' + fname)
    tar.extractall()
    tar.close()
    folder = os.path.splitext(os.path.splitext(os.path.basename(fname))[0])[0]
    print('importing ' + cwd + '/' + folder)
    sys.path.append(cwd + '/' + folder)

fh = open(zipName, 'rb')

# with zipfile.ZipFile(fh) as z:
z = zipfile.ZipFile(fh)

try:
    import progressbar
except:
    importModuleFromZip(0, z, 'python-utils-2.1.0.tar.gz')
    importModuleFromZip(1, z, 'progressbar-2.3.tar.gz')
    import progressbar

bar = progressbar.ProgressBar()
sflProgress = progressbar.ProgressBar()
delProgress = progressbar.ProgressBar()

if data:
    for k, v in bar(data.items()):
        head = v[0]
        source = head
        # print "copying %s" % (head)
        for tuple in v:
            relativeFilePathMatches = re.search(destinationRegex, tuple, re.UNICODE)
            destinationFolder = os.path.dirname(targetDirectory + "/" + startRealtive + relativeFilePathMatches.group(1))
            destinationName = os.path.basename(tuple)
            # print((destinationFolder + "/" + destinationName).encode('utf-8'))
            if not os.path.exists(destinationFolder):
                os.makedirs(destinationFolder)

            with open(destinationFolder + "/" + destinationName, 'wb') as f:
                name = "_files" + head
                f.write(z.read(name))
                # print "\t%s -> %s" % (head, (destinationFolder + "/" + destinationName))
                # print 'destination %s' % (destination)
                # print 'file %s' % name

                # z.extract(name, destination)
                # print 'source (%s) -> file %s' % ((source, tuple[0]))
    fh.close()

    #pull all of drupal if folder is empty on ship
    #else pull diffs and apply server side before transferring

    #pull all of nclcom if folder is empty on ship
    #else pull difs and apply server side before transferring

if softlinks:
    for src, dest in sflProgress(softlinks.items()):
        destination = convertLocation(src)
        source = convertLocation(dest)
        os.symlink(source, destination)
        # print("%s -> %s"%(source, destination))

if deletions:
    deletionErrors = []
    for src in delProgress(deletions):
        destination = convertSourceLocation(src)
        if os.path.isfile(destination):
            try:
                os.remove(destination)
            except OSError, e:  ## if failed, report it back to the user ##
                deletionErrors.append('Error: {0} - {1}.'.format(e.filename,e.strerror))
        else:  ## Show an error ##
            try:
                shutil.rmtree(destination)
            except OSError, e:
                deletionErrors.append('Error: {0} - {1}.'.format(e.filename,e.strerror))

    print '\n'.join(deletionErrors)
