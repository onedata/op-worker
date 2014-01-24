.. _dns_worker:

dns_worker
==========

	:Authors: Bartosz Polnik
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements :ref:`worker_plugin_behaviour` to provide functionality of resolution ipv4 addresses for given worker name.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <dns_worker;cleanup/0>`
	* :ref:`env_dependencies/0 <dns_worker;env_dependencies/0>`
	* :ref:`handle/2 <dns_worker;handle/2>`
	* :ref:`init/1 <dns_worker;init/1>`
	* :ref:`start_listening/0 <dns_worker;start_listening/0>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dns_worker

	.. _`dns_worker;cleanup/0`:

	.. erl:function:: cleanup() -> Result

	* **Result:** ok

	:ref:`worker_plugin_behaviour` callback cleanup/0

	.. _`dns_worker;env_dependencies/0`:

	.. _`dns_worker;handle/2`:

	.. _`dns_worker;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **Error:** term()
	* **Result:** #dns_worker_state{} | {error, Error}

	:ref:`worker_plugin_behaviour` callback init/1.

	.. _`dns_worker;start_listening/0`:

	.. erl:function:: start_listening() -> ok

	Starts dns listeners and terminates dns_worker process in case of error.

