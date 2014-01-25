.. _remote_files_manager:

remote_files_manager
====================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements worker_plugin_behaviour to provide functionality of remote files manager.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <remote_files_manager:cleanup/0>`
	* :ref:`handle/2 <remote_files_manager:handle/2>`
	* :ref:`init/1 <remote_files_manager:init/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`remote_files_manager:cleanup/0`:

	.. function:: cleanup() -> ok
		:noindex:

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback cleanup/0

	.. _`remote_files_manager:handle/2`:

	.. function:: handle(ProtocolVersion :: term(), Request :: term()) -> Result
		:noindex:

	* **Result:** term()

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback handle/1. <br/> Processes standard worker requests (e.g. ping) and requests from FUSE.

	.. _`remote_files_manager:init/1`:

	.. function:: init(Args :: term()) -> list()
		:noindex:

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback init/1

