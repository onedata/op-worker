.. _central_logger:

central_logger
==============

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements worker_plugin_behaviour to provide central logging functionalities.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <central_logger;cleanup/0>`
	* :ref:`handle/2 <central_logger;handle/2>`
	* :ref:`init/1 <central_logger;init/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: central_logger

	.. _`central_logger;cleanup/0`:

	.. erl:function:: cleanup() -> Result

	* **Error:** timeout | term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour` callback cleanup/0 Reconfigures lager back to standard

	.. _`central_logger;handle/2`:

	.. _`central_logger;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **Result:** {ok, term()}

	:ref:`worker_plugin_behaviour` callback init/1 Sets up the worker for propagating logs to CMT sessions and configures lager trace files.

