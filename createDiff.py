#!/usr/bin/python

import fileinput
import re
import pprint
import sys
from filecmp import dircmp
import hashlib
import zipfile
import datetime
import zipfile
import json
import os
import warnings
import argparse
import tarfile

cwd = os.path.dirname(os.path.realpath(__file__)) + "/"


def extractModule(fname):
    _cwd = os.getcwd()
    tar = tarfile.open(fname)
    tar.extractall()
    tar.close()
    folder = os.path.splitext(os.path.splitext(os.path.basename(fname))[0])[0]
    # print('importing ' + cwd + '/' + folder)
    print('importing ' + _cwd + '/' + folder)
    sys.path.append(_cwd + '/' + folder)

try:
    import progressbar
except:
    path = os.path.dirname(os.path.realpath(__file__))
    extractModule(path + '/progressbar-2.3.tar.gz')
    extractModule(path + '/python-utils-2.1.0.tar.gz')

    import progressbar


parser = argparse.ArgumentParser(description='Create a self extracting zip diff between source and target')

parser.add_argument('source',
                    metavar='<Source Folder>',
                    help='source folder')

parser.add_argument('target',
                    metavar='<Target folder>',
                    help='target folder')

parser.add_argument('--zipName',
                    dest='zipName',
                    default='differential.zip',
                    help='The name of the zip file to be created from the differential output')

parser.add_argument('--destinationPath',
                    dest='destFolderName',
                    help="The full path of the destination folder that the contents will be extracted to.")


args = parser.parse_args()
source = args.source
target = args.target
zipName = args.zipName
destinationFolder = args.destFolderName if args.destFolderName is not None else target



def print_info(archive_name):
    with zipfile.ZipFile(archive_name) as zf:
        for info in zf.infolist():
            print(info.filename)
            print('  Comment     :', info.comment)
            mod_date = datetime.datetime(*info.date_time)
            print('  Modified    :', mod_date)
            if info.create_system == 0:
                system = 'Windows'
            elif info.create_system == 3:
                system = 'Unix'
            else:
                system = 'UNKNOWN'
            print('  System      :', system)
            print('  ZIP version :', info.create_version)
            print('  Compressed  :', info.compress_size, 'bytes')
            print('  Uncompressed:', info.file_size, 'bytes')
            print()

#TODO preserve empty directories

def print_diff_files(dcmp, symLinks, deletions):
    hashMap = {}
    deletions.extend([ dcmp.left + "/" + s for s in dcmp.left_only])
    # print "deletions %s " % (dcmp.left_only)
    for name in dcmp.right_only + dcmp.diff_files:
        # 		print "diff_file %s found in %s and %s" % (name, dcmp.left, dcmp.right)

        # Open,close, read file and calculate MD5 on its contents
        # if not os.path.isdir(dcmp.left + "/" + name):
        # if os.path.isdir(dcmp.left + "/" + name):
        #     return scan_dir(dcmp.left + "/" + name, hashMap)
        # else:
        try:
            if not name.startswith('.'):  # ignore hidden files
                if not os.path.islink(dcmp.right + "/" + name):
                    with open(dcmp.right + "/" + name) as file_to_check:
                        # read contents of the file
                        data = file_to_check.read()
                        # pipe contents of the file through
                        md5_returned = hashlib.md5(data).hexdigest()
                        #	    relativeFilePathMatches = re.search(relativePathRegex, dcmp.left, re.UNICODE)
                        #	if relativeFilePathMatches:
                        hashMap.setdefault(md5_returned, []).append((dcmp.right + "/" + name).decode("UTF-8"))
                        # print "md5 %s file %s found only in %s with " % (md5_returned, name, dcmp.left)
                else:
                    symLinks[dcmp.right + "/" + name] = os.path.realpath(dcmp.right + "/" + name)
        except IOError:
            if not os.path.islink(dcmp.right + "/" + name):
                if os.path.isdir(dcmp.right + "/" + name):
                    # print "%s left only" % (dcmp.left + "/" + name)
                    scan_dir(dcmp.right + "/" + name, hashMap, symLinks)
            else:
                symLinks[dcmp.right + "/" + name] = os.path.realpath(dcmp.right + "/" + name)

    for sub_dcmp in dcmp.subdirs.values():
        hashMap.update(print_diff_files(sub_dcmp, symLinks, deletions))

    return hashMap


def scan_dir(dir, hashMap, symLinks):
    for name in os.listdir(dir):
        path = os.path.join(dir, name)
        if not os.path.islink(path):
            if os.path.isfile(path):
                data = open(path, 'rb').read()
                md5_returned = hashlib.md5(data).hexdigest()
                hashMap.setdefault(md5_returned, []).append((dir + "/" + name).decode("UTF-8"))
                # print path
            else:
                scan_dir(path, hashMap, symLinks)
        else:
            symLinks[path] = os.path.realpath(path)


# print "diff", sys.argv[1], sys.argv[2]

print 'comparing directories...'
symLinks = {}
deletions = []

result = print_diff_files(dircmp(source, target), symLinks, deletions)
dict = result
print 'creating archive...'

# try:
#     try:
#         import zlib
#         warnings.filterwarnings('ignore')
#
#         compression = zipfile.ZIP_DEFLATED
#     except:
#         compression = zipfile.ZIP_STORED

# modes = {zipfile.ZIP_DEFLATED: 'deflated', zipfile.ZIP_STORED: 'stored'}
# zf = zipfile.ZipFile('zipfile_write.zip', mode='w', allowZip64 = True)


# zf.write("__main__.py")
# zf.write("__init__.py")
# zf.write("__init__.py", "_files/__init__.py")
# zf.writestr("_files/target", sys.argv[2])



# with progressbar.ProgressBar(maxval=10) as bar:
try:
    import zlib
    warnings.filterwarnings('ignore')
    compression = zipfile.ZIP_DEFLATED
except:
    compression = zipfile.ZIP_STORED

with zipfile.ZipFile(zipName, mode='w', compression=compression, allowZip64 = True) as zf:
    initialized = {}
    i = 0
    bar = progressbar.ProgressBar()

    if (len(dict.keys()) > 0):
        for k, v in bar(dict.items()):
            tuple = v[0]
            initAll = tuple.split('/')
            index = 1

            key = ''
            for p in initAll[1:-1]:
                key += "/" + p
                if key not in initialized:
                    zf.write(cwd + "__init__.py", "_files" + key + "/__init__.py")
                initialized.setdefault(key, [])
            zf.write(tuple, "_files" + tuple , compress_type=compression)

    jase = json.dumps(dict, indent=4, sort_keys=True)
    print 'writing manifest'
    zf.writestr("_files/manifest.json", jase)
    zf.writestr("_files/source", source)
    zf.writestr("_files/target", destinationFolder)
    zf.writestr("_files/zipName", zipName)
    zf.writestr("_files/symLinks.json", json.dumps(symLinks, indent=4, sort_keys=True))
    zf.writestr("_files/deletions.json", json.dumps(deletions, indent=4, sort_keys=True))


    zf.write(cwd + "__main__.py", "__main__.py")
    zf.write(cwd + "__init__.py", "__init__.py")
    zf.write(cwd + "__init__.py", "_files/__init__.py")
    zf.write(cwd + "progressbar-2.3.tar.gz", "progressbar-2.3.tar.gz")
    zf.write(cwd + "python-utils-2.1.0.tar.gz", "python-utils-2.1.0.tar.gz")

    print 'closing'
    zf.close()



