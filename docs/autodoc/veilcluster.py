# -*- coding: utf-8 -*-

# @author Krzysztof Trzepla
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license cited in 'LICENSE.txt'.
# @doc: This script creates rst files from veilclient source code using erl_doc_lexer. @end

import sys
import os
import re
import subprocess
from sphinx import Sphinx

class VeilCluster(Sphinx):

	def __init__(self, root, path):
		self.root = root
		self.path = path
		self.links = {'"README.md"' : ':doc:`about`', '"README-DEVELOPER.md"' : ':doc:`development`'}
		self.excluded = ['mochijson2.erl', 'mochinum.erl']

	def autodoc(self):
		if not os.path.exists(self.path):
			os.makedirs(self.path)

		self.format_links(self.path, 'about.rst')
		self.format_links(self.path, 'development.rst')

		self.doc_erlang()
		self.doc_c()
		self.create_toctree(self.path, 1)

	def doc_erlang(self):
		tree = os.walk(self.root + '/src')
		for root, dirs, files in tree:
			for file in files:
				if file not in self.excluded and file.endswith(".erl"):
					path = root.replace(self.root + '/src', self.path + '/erlang_modules')
					if not os.path.exists(path):
						os.makedirs(path)
					writer = ErlangDocWriter(ErlangDocLexer(os.path.join(root, file)), path)
					writer.create_rst_document()

	def doc_c(self):
		tree = os.walk(self.root + '/c_src')
		for root, dirs, files in tree:
			for file in files:
				if file.endswith(".md"):
					path = root.replace(self.root + '/c_src', self.path + '/c_modules')
					if not os.path.exists(path):
						os.makedirs(path)
					# TODO: uncomment when pandoc is installed
					# process = subprocess.Popen(['pandoc', '--from=markdown', '--to=rst', '--output=' + path + '/' + file[:-3] + '.rst', root + '/' + file], stdout=sys.stdout, stderr=sys.stderr)
					# process.wait()


class Details(object):

	def __init__(self):
		self.declaration = None
		self.descritpion = None
		self.parameters = {}


class ErlangDocLexer(object):

	def __init__(self, filename):
		with open(filename, 'r') as file:
			self.content = file.read()

	def lex(self):
		self.module = self.lex_declaration(r'-module')
		self.behaviour = self.lex_declaration(r'-behaviour')
		self.author = self.lex_info(r'%% @author')
		self.copyright = 'This software is released under the :ref:`license`.'
		self.descritpion = self.lex_info(r'%% @doc:')
		self.functions = self.lex_functions()
		self.details = self.lex_details()

	def lex_info(self, type):
		pattern = re.compile(type + r'(.*?)(%% @|-spec)', re.DOTALL)
		match = re.search(pattern, self.content)
		if not match:
			return None
		info = self.lex_links(self.format_whitespaces(match.group(1)))
		return info

	def format_whitespaces(self, string):
		string = ' '.join(re.split(r'[\s%]+', string.strip()))
		return string

	def lex_declaration(self, type):
		pattern = re.compile(type + r'\s*\((.*?)\)')
		match = re.search(pattern, self.content)
		if not match:
			return None
		return match.group(1)

	def lex_functions(self):
		functions = []
		pattern = re.compile(r'-ifdef\(TEST\).*?-endif', re.DOTALL)
		content = re.sub(pattern, '', self.content)
		pattern = re.compile(r'-export\(\[(.*?)\]\)', re.DOTALL)
		iterator = pattern.finditer(content)
		for match in iterator:
			functions = functions + re.split(r'[\s,]+', match.group(1).strip())
		return functions

	def lex_links(self, string):
		pattern = re.compile(r'\s*<[^>]*>[^>]\s*')
		string = re.sub(pattern, ' ', string)
		string = string.replace('`', '\'')
		links = []
		pattern = re.compile(r'{@link (.*?)}')
		iterator = pattern.finditer(string)
		for match in iterator:
			links.append(match.group(1))
		for link in links:
			string = string.replace('{@link ' + link + '}', ':ref:`' + link + ' <' + link + '>`')
		return string

	def lex_spec(self, function):
		pattern = re.compile(r'%% ' + function + r'.*?-spec(.*?)\.', re.DOTALL)
		match = re.search(pattern, self.content)
		if not match:
			return (None, None)
		match = match.group(1).split('when')
		declaration = self.format_whitespaces(match[0])
		parameters = {}
		if len(match) == 1:
			parameters = None
		else:
			params = re.split(r',\s*?\n', match[1])
			for param in params:
				p = param.split('::', 1)
				parameters[self.format_whitespaces(p[0])] = self.format_whitespaces(p[1])
		return (declaration, parameters)

	def lex_details(self):
		details = {}
		for function in self.functions:
			details[function] = Details()
			details[function].descritpion = self.lex_info(r'%% ' + function + r'.*?@doc')
			details[function].declaration, details[function].parameters = self.lex_spec(function)
		return details


class ErlangDocWriter(Sphinx):

	def __init__(self, lexer, path):
		self.lexer = lexer
		self.path = path
		self.behaviours = ['gen_server', 'gen_event', 'supervisor', 'application']

	def create_rst_document(self):
		self.lexer.lex()
		with open(self.path + '/' + self.lexer.module + '.rst', 'w') as file:
			self.w_reference(file, self.lexer.module, 0)
			self.w_section(file, self.lexer.module, '=')
			self.w_description(file)
			self.w_function_index(file)
			self.w_function_details(file)

	def w_description(self, file):
		if self.lexer.author:
			file.write('\t:Authors: ' + self.lexer.author + '\n')
		file.write('\t:Copyright: ' + self.lexer.copyright + '\n')
		if self.lexer.descritpion:
			file.write('\t:Descritpion: ' + self.lexer.descritpion + '\n')
		if self.lexer.behaviour:
			if self.lexer.behaviour in self.behaviours:
				file.write('\t:Behaviours: `' + self.lexer.behaviour + ' <http://www.erlang.org/doc/man/' + self.lexer.behaviour + '.html>`_\n')
			elif self.lexer.behaviour == 'ranch_protocol':
				file.write('\t:Behaviours: `ranch_protocol <https://github.com/extend/ranch/blob/master/manual/ranch_protocol.md>`_\n')
			else:
				file.write('\t:Behaviours: :ref:`' + self.lexer.behaviour + '`\n')

	def w_function_index(self, file):
		if self.lexer.functions:
			self.w_section(file, '\nFunction Index', '~')
			self.lexer.functions.sort()
			for function in self.lexer.functions:
				file.write('\t* :ref:`' + function + ' <' + self.lexer.module + ':' + function + '>`\n')

	def w_function_details(self, file):
		if self.lexer.functions:
			self.w_section(file, '\nFunction Details', '~')
			for function in self.lexer.functions:
				file.write('\t.. _`' + self.lexer.module + ':' + function + '`:\n\n')
				if self.lexer.details[function].declaration:
					file.write('\t.. function:: ' + self.lexer.details[function].declaration + '\n')
					file.write('\t\t:noindex:\n\n')
				if self.lexer.details[function].parameters:
					for name, value in sorted(self.lexer.details[function].parameters.iteritems()):
						file.write('\t* **' + name + ':** ' + value + '\n')
					file.write('\n')
				if self.lexer.details[function].descritpion:
					file.write('\t' + self.lexer.details[function].descritpion + '\n\n')

if __name__ == '__main__':

	if len(sys.argv) != 3:
		print "Wrong number of parameters"
		print "Usage: python {0} path_to_root_folder path_to_veilcluster_folder".format(sys.argv[0])
		exit(1)

	veilcluster = VeilCluster(sys.argv[1], sys.argv[2])
	veilcluster.autodoc()
