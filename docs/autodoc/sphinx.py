# -*- coding: utf-8 -*-

# @author Krzysztof Trzepla
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license cited in 'LICENSE.txt'.
# @doc: This script creates rst files from veilclient source code using erl_doc_lexer. @end

import os

class Sphinx(object):

	def w_section(self, file, name, char):
		file.write(name + '\n' + char * len(name) + '\n\n')	

	def w_reference(self, file, name, depth):
		file.write('\t' * depth + '.. _' + name + ':\n\n')

	def w_toctree(self, file, depth, maxdepth=2):
		file.write('\t' * depth + '.. toctree::\n')
		file.write('\t' * (depth + 1) + ':maxdepth: ' + str(maxdepth) + '\n\n')

	def w_include(self, file, name, depth):
		file.write('\t' * depth + '.. include:: ' + name + '\n')

	def w_class(self, file, name, depth):
		file.write('\t' * depth + '.. doxygenclass:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n')
		file.write('\t' * (depth + 1) + ':members:\n')
		file.write('\t' * (depth + 1) + ':sections: public*, protected*, private*\n\n')

	def w_struct(self, file, name, depth):
		file.write('\t' * depth + '.. doxygenstruct:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def w_function(self, file, name, depth):
		file.write('\t' * depth + '.. doxygenfunction:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def w_enum(self, file, name, depth):
		file.write('\t' * depth + '.. doxygenenum:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def w_typedef(self, file, name, depth):
		file.write('\t' * depth + '.. doxygentypedef:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def w_define(self, file, name, depth):
		file.write('\t' * depth + '.. doxygendefine:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def w_variable(self, file, name, depth):
		file.write('\t' * depth + '.. doxygenvariable:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def w_file(self, file, name, depth):
		file.write('\t' * depth + '.. doxygenfile:: ' + name + '\n')
		file.write('\t' * (depth + 1) + ':project: veilclient\n\n')

	def format_links(self, path, rstfile):
		with open(self.path + '/' + rstfile, 'r') as f:
			content = f.read()
		for oldlink, newlink in self.links.iteritems():
			content = content.replace(oldlink, newlink)
		with open(self.path + '/' + rstfile, 'w') as f:
			f.write(content)

	def create_toctree(self, path, lowerbound):
		name = path.split('/')[-1]
		name = name.replace('_', ' ')
		name = name[0].upper() + name[1:]
		files = []
		dirs = []
		for file in os.listdir(path):
			if file == 'index.rst':
				return
			elif os.path.isfile(path + '/' + file):
				files.append(file)
			elif os.path.isdir(path + '/' + file):
				subfiles = os.listdir(path + '/' + file)
				if len(subfiles) > lowerbound:
					dirs.append(file)
				else:
					for subfile in subfiles:
						files.append(file + '/' + subfile)

		with open(path + '/index.rst', 'w') as f:
			self.w_section(f, name, '=')
			if files:
				f.write("**Files**\n\n")
				self.w_toctree(f, 0, 1)
				for file in sorted(files):
					f.write('\t' + file[:-4] + '\n')
			if dirs:
				f.write("\n**Submodules**\n\n")
				self.w_toctree(f, 0)
				for dir in sorted(dirs):
					f.write('\t' + dir + '/index\n')
					self.create_toctree(path + '/' + dir, lowerbound)
