# -*- coding: utf-8 -*-

"""
Copyright (C) 2016. xAd, Inc.  All Rights Reserved.
"""

import logging
import os.path
import re


class Conf(object):
    """Configuration

    This is meant to be the Python equivalent of the
    xAd Perl Conf.
    """

    def __init__(self):
        self.dirs = []
        self.prefix = None
        self.props = {}
        self.emptyPattern = re.compile("^\s*$")
        self.commentPattern = re.compile("^\s*#")
        # Key = value pattern
        self.kvPattern = re.compile("([^=\s]+)\\s*=\\s*(.*)")
        # Parameters like ${proj.name} or $(proj.root)
        self.varPattern = re.compile("[^\$]*(\$[\(\{][^\(\{}]+[\)\}])")
        # Include pattern
        self.incPattern = re.compile("include\s*=\s*(.*)")
        self.optPattern = re.compile("(.+?)\s*\[(.+)\]")
        self.arrayPattern = re.compile("^\[?\s*(.*?)\s*\]?$")

    def load(self, filename, dirs=""):
        """Load filename from specified dirs.
        dirs is a list of directories delimited by ":" (like PATH)"""
        logging.debug('Loading {}'.format(filename))
        items = dirs.split(":")
        for item in reversed(items):
            self.dirs.insert(0, item)
        self.load_file(filename)

    def load_file(self, filename, optional=False):
        """Load the specified filename"""

        # Get the full file path.  Can be None.
        path = self._get_path(filename)

        if path and os.path.isfile(path):
            # Debug message
            if (optional):
                logging.debug('- Loading {} [optional]'.format(path))
            else:
                logging.debug('- Loading {}'.format(path))

            ext = os.path.splitext(filename)[-1]
            if (ext == ".properties"):
                self.load_properties(path)
            elif (ext == ".yml" or ext == ".yaml"):
                logging.warn('X SKIP {}'.format(filename))
                # self.load_properties(path)
                pass
            else:
                logging.warn("UNKNOWN FILE EXTENSION %s", ext)
                self.load_properties(path)
        elif optional:
            logging.info('X SKIP {} [optional]'.format(filename))
        else:
            logging.error("File not found: '{}'".format(filename))
            logging.exception("File not found %s", str(filename))

    def _get_path(self, filename):
        """Find filename in given dir list"""
        path = None

        if (os.path.isfile(filename)):
            dirname = os.path.dirname(filename)
            self.dirs.insert(0, dirname)
            path = filename
        else:
            for dirname in self.dirs:
                tmp = os.path.join(dirname, filename)
                if (os.path.isfile(tmp)):
                    path = tmp
                    break
        return path

    def load_properties(self, path):
        """Load a properties file"""
        # logging.info("Loading '%s'...", path)

        multiLine = False
        processLine = ""
        with open(path, 'r') as ifs:
            for line in ifs:
                # Strip the line and skip comment lines
                line = line.strip()
                if self._is_comment_or_empty(line):
                    continue

                if self._handle_include(line):
                    continue

                # Check of multi-line characters
                if (line[-1] == "\\"):
                    line = line[:-1]
                    if (not multiLine):
                        processLine = line
                        multiLine = True
                    else:
                        processLine += line
                else:
                    processLine += line
                    multiLine = False

                if not multiLine:
                    self._process_properties_line(processLine)
                    processLine = ""

            if (processLine != ""):
                self._process_properties_line(processLine)

    def _is_comment_or_empty(self, line):
        """Check if a line is a comment line or an empty line"""
        retval = False
        if self.emptyPattern.match(line) or self.commentPattern.match(line):
            retval = True
        return (retval)

    def _handle_include(self, line):
        """Handle the include directive"""
        retval = False
        isOptional = False
        # Search for "include = ..."
        m1 = self.incPattern.match(line)
        if (m1):
            filename = m1.group(1)
            # Get the optional argument in [optional]
            m2 = self.optPattern.match(filename)
            if m2:
                filename = m2.group(1)
                opt = m2.group(2)
                if opt.find("optiona") >= 0:
                    isOptional = True

            # Load the include file
            self.load_file(filename, isOptional)
            retval = True

        return retval

    def _process_properties_line(self, line):
        """Process a complete line in the properties file"""
        m = self.kvPattern.match(line)
        if (m):
            key = m.group(1)
            val = m.group(2)
            self.props[key] = val
        else:
            logging.error("Invalid line '%s'", line)

    def load_yaml(self, path):
        """Load a yaml file"""
        # TBD
        pass

    def has(self, key):
        """Check if the specified key is dfined"""
        return (key in self.props)

    def get(self, key, alt=None):
        """Get the value of the specified key.

        It optionally takes an alternative key/value which will be
        applied when the key is not found.
        It will throws exception if key is not found and an alternative
        is not provded.
        """
        val = None
        if key not in self.props:
            # Check for alternative, which can be either key or value
            if (alt is not None):
                if self.has(alt):
                    val = self.get(alt)
                else:
                    val = alt
            # Throws exception if the key is not defined.
            else:
                logging.error("Key not found: '%s'", key)
                raise NameError("Key not found: " + key)
        else:
            val = self.props[key]

            # Expand parameters
            m = self.varPattern.match(val)
            while (m):
                start = m.start(1)
                end = m.end(1)
                var = m.group(1)
                mkey = var[2:-1]
                mval = self.get(mkey)
                val = val[:start] + mval + val[end:]
                m = self.varPattern.match(val)
        return val

    def get_float(self, key, alt=None):
        return float(self.get(key, alt))

    def get_in_context(self, key, context):
        """Get a parameter in the specified context"""
        # TBD
        pass

    def get_array(self, key, alt=None):
        """Return the value as an array (list).   Elements are delimited
        by ',' followed by zero or many spaces.   The whole array string
        can be optionall wrapped by [ ]. For example:
            a1 = 1, 2, 3, 4, 5
            a2 = [6,7,8,9,10]
        """
        retval = None
        rawVal = self.get(key, alt)
        # Remove optional brackets
        m = self.arrayPattern.match(rawVal)
        if (m):
            val = m.group(1)
            retval = re.split("[,\s]+", val)
        else:
            logging.warn("Not array: " + rawVal)
        return retval

    def dump(self):
        """Dump the contents"""
        logging.info('# Dump')
        logging.info("+ Dirs = " + str(self.dirs))
        if (self.prefix is not None):
            logging.info(" + Prefix = " + str(self.prefix))
        logging.info("+ Parameters:")
        keylist = self.props.keys()
        keylist.sort()
        for k in keylist:
            v = self.get(k)
            logging.info("  - {} => {}".format(k, v))
        logging.info("---")

    def subset(self, prefix):
        """Create a subset configuration.
        Create a new configuration containing keys that start with prefix.
        Leading periods will be removed from the keys.

        For example, if the original configuration has foo.x1, foo.x2.
        subset('foo') will extract these two properties by the keys will 
        be renamed to x1 and x2.
        """
        n = len(prefix)
        subConf = Conf()
        subConf.dirs = self.dirs
        subConf.prefix = prefix if (self.prefix is None) \
            else ".".join(self.prefix, prefix)
        keylist = self.props.keys()
        for k in keylist:
            if (k.startswith(prefix) and k[n] == '.'):
                newkey = k[n+1:]
                val = self.get(k)
                subConf.props[newkey] = val;
        return subConf


def _test1():
    """Load the full coniguration"""
    logging.info("### TEST1 ###")
    mainDirs = "../../../config:../../../../test/config"
    c2 = Conf()
    c2.load("spring.properties", mainDirs)
    c2.dump()


def _test2():
    """Load test configurations"""
    logging.info("### TEST2 ###")
    testDirs = ".:../../../../test/config"
    configFile = "conf_test1.properties"
    c = Conf()
    c.load(configFile, testDirs)
    c.dump()
    logging.debug("Test get/has")
    for key in ["proj.root", "proj.name", "proj.home", "proj.data.dir"]:
        val = c.get(key)
        logging.debug("  - get({}) => '{}'".format(key, val))
    for key in ["proj.root", "xxx", "yyyy"]:
        logging.debug("  - has({}) = {}".format(key, c.has(key)))

    # Test Arrays
    for key in ["a1", "a2"]:
        val = c.get_array(key)
        logging.debug("  - get_array({}) = {}".format(key, val))

    # Subset
    logging.info("Getting subset('proj')...")
    subConf = c.subset('proj')
    subConf.dump()


def main():
    """ Unit Test.  Run from the current directory."""
    # Set up logging
    fmt = ("[%(module)s:%(funcName)s] %(levelname)s %(message)s")
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=fmt, datefmt=datefmt, level=logging.DEBUG)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # IPython specific setting
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    _test1()
    _test2()
    logging.info('Done!')


if (__name__ == "__main__"):
    """Unit Test"""
    main()
