"""Author: Jakub Kudzia
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

This module contains util functions used in ct_run.py and test_run.py
"""

import glob
import xml.etree.ElementTree as ElementTree


def skipped_test_exists(junit_report_path):
    reports = glob.glob(junit_report_path)
    # if there are many reports, check only the last one
    reports.sort()
    tree = ElementTree.parse(reports[-1])
    testsuites = tree.getroot()
    for testsuite in testsuites:
        if testsuite.attrib['skipped'] != '0':
            return True
    return False