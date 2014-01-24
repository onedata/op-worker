.. _control_panel:

control_panel
=============

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements worker_plugin_behaviour callbacks. It is responsible for setting up cowboy listener and registering nitrogen_handler as the handler.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <control_panel;cleanup/0>`
	* :ref:`handle/2 <control_panel;handle/2>`
	* :ref:`init/1 <control_panel;init/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: control_panel

	.. _`control_panel;cleanup/0`:

	.. erl:function:: cleanup() -> Result

	* **Error:** timeout | term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour` callback cleanup/0 Stops cowboy listener and terminates

	.. _`control_panel;handle/2`:

	.. _`control_panel;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **Error:** term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour` callback init/1 Sets up cowboy dispatch with nitrogen handler and starts cowboy service on desired port.

