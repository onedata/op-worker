.. _control_panel:

control_panel
=============

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements worker_plugin_behaviour callbacks. It is responsible for setting up cowboy listener and registering nitrogen_handler as the handler.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <control_panel:cleanup/0>`
	* :ref:`handle/2 <control_panel:handle/2>`
	* :ref:`init/1 <control_panel:init/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`control_panel:cleanup/0`:

	.. function:: cleanup() -> Result
		:noindex:

	* **Error:** timeout | term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback cleanup/0 <br /> Stops cowboy listener and terminates

	.. _`control_panel:handle/2`:

	.. _`control_panel:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Error:** term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback init/1 <br /> Sets up cowboy dispatch with nitrogen handler and starts cowboy service on desired port.

