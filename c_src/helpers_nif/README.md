HelpersNIF
==========

	HelpersNIF is an NIF wrapper for Helpers library. 

### Files

	* **helpers_nif.cc** - NIF wrapper for IStorageHelper interface (see Helper source for more info).

	* **term_translator.cc** - Helper functions used to translate Erlang terms to c/c++ types

### Prerequisites

	All you need are Erlang NIF libraries and headers. Normally they are shipped with Erlang.

### Compilation

	The wrapper should be compiled as a part of cluster using **rebar**, because rebar automatically adds required by 
	Elang NIF API lib/headers path to compiler options.