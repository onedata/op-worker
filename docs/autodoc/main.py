# -*- coding: utf-8 -*-

# @author Krzysztof Trzepla
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license cited in 'LICENSE.txt'.
# @doc: This script creates rst files from veilclient source code using erl_doc_lexer. @end

import sys
import os
from sphinx import Sphinx

class Main(Sphinx):

	def __init__(self, path):
		self.path = path

	def autodoc(self):
		if not os.path.exists(self.path):
			os.makedirs(self.path)

		docs = ['veilClient', 'veilCluster']
		with open(self.path + '/index.rst', 'w') as index:
			self.w_section(index, 'VeilFS', '=')
			self.w_toctree(index, 1)
			for doc in docs:
				index.write('\t\t' + doc + '/index\n')
			index.write('\t\tlicense\n')

		self.doc_license()

	def doc_license(self):
		with open(self.path + '/license.rst', 'w') as license:
			self.w_reference(license, 'license', 0)
			self.w_include(license, '../LICENSE.txt', 0)


if __name__ == '__main__':

	if len(sys.argv) != 2:
		print "Wrong number of parameters"
		print "Usage: python {0} path_to_main_folder".format(sys.argv[0])
		exit(1)

	root = Main(sys.argv[1])
	root.autodoc()
