# -*- coding: utf-8 -*-

# @author Krzysztof Trzepla
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license cited in 'LICENSE.txt'.
# @doc: This script creates rst files from veilclient source code using erl_doc_lexer. @end

import sys
import os
import re
from xml.dom import minidom
from sphinx import Sphinx

class VeilClient(Sphinx):

	def __init__(self, main, path):
		self.main = main
		self.path = path
		self.doxygenKinds = ['function', 'struct', 'enum', 'typedef', 'define', 'variable', 'class', 'file']

	def autodoc(self):
		if not os.path.exists(self.path):
			os.makedirs(self.path)

		self.index = minidom.parse(self.main + '/data/veilclient/xml/index.xml').getElementsByTagName('compound')
		self.classes = filter(lambda node: node.getAttribute('kind') == 'class' and len(node.childNodes) > 2, self.index)
		self.structs = filter(lambda node: node.getAttribute('kind') == 'struct' and len(node.childNodes) > 2, self.index)
		self.namespaces = filter(lambda node: node.getAttribute('kind') == 'namespace' and len(node.childNodes) > 2, self.index)
		self.files = filter(lambda node: node.getAttribute('kind') == 'file' and len(node.childNodes) > 2, self.index)

		docs = ['classes', 'files', 'namespaces', 'structs']
		for doc in docs:
			getattr(self, 'doc_' + doc)()

		self.create_toctree(self.path, 1)

	def doc_classes(self):
		if not os.path.exists(self.path + '/classes/'):
			os.makedirs(self.path + '/classes')

		for cls in self.classes:
			name = cls.getElementsByTagName('name')[0].firstChild.nodeValue
			with open(self.path + '/classes/' + name.replace('::', '.') + '.rst', 'w') as f:
				self.w_section(f, name, '=')
				self.w_class(f, name, 1)

	def doc_structs(self):
		if not os.path.exists(self.path + '/structs/'):
			os.makedirs(self.path + '/structs/')

		for struct in self.structs:
			name = struct.getElementsByTagName('name')[0].firstChild.nodeValue
			with open(self.path + '/structs/' + name.replace('::', '.') + '.rst', 'w') as f:
				self.w_section(f, name, '=')
				self.w_struct(f, name, 1)

	def doc_namespaces(self):
		if not os.path.exists(self.path + '/namespaces/'):
			os.makedirs(self.path + '/namespaces/')

		for namespace in self.namespaces:
			name = namespace.getElementsByTagName('name')[0].firstChild.nodeValue
			with open(self.path + '/namespaces/' + name.replace('::', '.') + '.rst', 'w') as f:
				self.w_section(f, name, '=')
				for member in namespace.getElementsByTagName('member'):
					kind = member.getAttribute('kind')
					if kind in self.doxygenKinds:
						getattr(self, 'w_' + kind)(f, member.getElementsByTagName('name')[0].firstChild.nodeValue, 1)

	def doc_files(self):
		if not os.path.exists(self.path + '/files/'):
			os.makedirs(self.path + '/files/')

		globusProxyUtilsMembers = ['fmemopen.c', 'fmemopen.h', 'globus_stdio_ui.c', 'globus_stdio_ui.h',\
		'grid_proxy_info.c', 'grid_proxy_init.c', 'gsi_utils.h']
		src = []
		include = []

		for file in self.files:
			name = file.getElementsByTagName('name')[0].firstChild.nodeValue
			if name not in globusProxyUtilsMembers and name.endswith('.cc'):
				src.append(name)
			if name not in globusProxyUtilsMembers and name.endswith('.h'):
				include.append(name)

		with open(self.path + '/files/index.rst', 'w') as index:
			self.w_section(index, 'Files', '=')
			index.write('**Source code**\n\n')
			self.w_toctree(index, 0)
			for name in src:
				index.write('\t' + name.replace('::', '.') + '\n')
				with open(self.path + '/files/' + name.replace('::', '.') + '.rst', 'w') as f:
					self.w_section(f, name, '=')
					self.w_file(f, name, 1)
			index.write('\n')

			index.write('**Headers**\n\n')
			self.w_toctree(index, 0)
			for name in include:
				index.write('\t' + name.replace('::', '.') + '\n')
				with open(self.path + '/files/' + name.replace('::', '.') + '.rst', 'w') as f:
					self.w_section(f, name, '=')
					self.w_file(f, name, 1)
			index.write('\n')

			index.write('**Globus proxy utils**\n\n')
			self.w_toctree(index, 0)
			for name in globusProxyUtilsMembers:
				index.write('\t' + name.replace('::', '.') + '\n')
				with open(self.path + '/files/' + name.replace('::', '.') + '.rst', 'w') as f:
					self.w_section(f, name, '=')
					self.w_file(f, name, 1)
			index.write('\n')


if __name__ == '__main__':

	if len(sys.argv) != 3:
		print "Wrong number of parameters"
		print "Usage: python {0} path_to_main_folder path_to_veilclinent_folder".format(sys.argv[0])
		exit(1)

	veilclient = VeilClient(sys.argv[1], sys.argv[2])
	veilclient.autodoc()
