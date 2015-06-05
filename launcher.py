import pkgutil
import sys
import os
# Our analysis modules
import modules
import getopt
import shutil
import errno
import data_import


def list_datasets(pathprefix):

    print 'The list of files present under %s' % (pathprefix)
    res = os.listdir(pathprefix)
    for r in res:
        if(r.endswith('parquet') or r.endswith('h5')):
            print(r)


def list_modules():

    package = modules
    prefix = package.__name__ + "."
    i = 1
    for importer, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
        print "Module %d : %s (Package? %s)" % (i, modname, ispkg)
        # module = __import__(modname, fromlist="dummy")
        # print "Imported", module
        i = i + 1


def import_modules(src, dst):
    if(src.endswith('/')):
        src = src[:-1]
    if(not dst.endswith('/')):
        dst = dst + '/'

    filename = os.path.basename(src)
    dst = dst + filename
    try:
        shutil.copytree(src, dst)
    except OSError as exc:  # If the module is not a package
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else:
            raise


def main(argv):

    inputfile = ''
    outputfile = ''
    print(argv)
    try:
        opts, args = getopt.getopt(argv, "hld:a:c:i:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print 'Not recognized option'
        sys.exit(2)
    print(opts)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt == '-l':
            list_modules()
            sys.exit()
        elif opt == '-d':
            pathprefix = arg
            list_datasets(pathprefix)
            sys.exit()
        elif opt == '-m':
            modulepath = arg
            import_modules(modulepath, 'modules')
        elif opt == '-c':
            filepath = arg
            data_import.main(["CANCELS", filepath])
        # elif opt == '-a':
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg


if __name__ == '__main__':
    main(sys.argv[1:])
