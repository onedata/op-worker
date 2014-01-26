.. _veilhelpers:

veilhelpers
===========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module shoul be used as an proxy to veilhelpers_nif module. This module controls way of accessing veilhelpers_nif methods.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`exec/2 <veilhelpers:exec/2>`
	* :ref:`exec/3 <veilhelpers:exec/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`veilhelpers:exec/2`:

	.. _`veilhelpers:exec/3`:

	.. function:: exec(Method :: atom(), SHInfo :: #storage_helper_info{}, [Arg :: term()]) -> {error, Reason :: term()} | Response
		:noindex:

	* **Response:** term()

	Executes apply(veilhelper_nif, Method, Args) through slave node. Before executing, fields from struct SHInfo are preappend to Args list. You can also skip SHInfo argument in order to pass exact Args into target Method.

