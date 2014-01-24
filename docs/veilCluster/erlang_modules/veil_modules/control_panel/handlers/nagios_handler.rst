.. _nagios_handler:

nagios_handler
==============

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module handles Nagios monitoring requests.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`handle/2 <nagios_handler:handle/2>`
	* :ref:`init/3 <nagios_handler:init/3>`
	* :ref:`terminate/3 <nagios_handler:terminate/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`nagios_handler:handle/2`:

	.. function:: handle(term(), term()) -> {ok, term(), term()}
		:noindex:

	Handles a request producing an XML response

	.. _`nagios_handler:init/3`:

	.. function:: init(any(), term(), any()) -> {ok, term(), []}
		:noindex:

	Cowboy handler callback, no state is required

	.. _`nagios_handler:terminate/3`:

	.. function:: terminate(term(), term(), term()) -> ok
		:noindex:

	Cowboy handler callback, no cleanup needed

