VeilHelpersNIF
==============

VeilHelpersNIF is an NIF wrapper for VeilHelpers library.

Files
~~~~~

-  **veilhelpers\_nif.cc** - NIF wrapper for IStorageHelper interface
   (see VeilHelper source for more info).

-  **term\_translator.cc** - Helper functions used to translate erlang
   terms to c/c++ types

Prerequisites
~~~~~~~~~~~~~

All you need are Erlang NIF libraries and headers. Normally they are
shipped with Erlang.

Compilation
~~~~~~~~~~~

The wrapper should be compiled as a part of cluster using **rebar**,
because rebar automatically adds required by Elang NIF API lib/headers
path to compiler options.
