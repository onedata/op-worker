.. _veil_cluster_node_app:

veil_cluster_node_app
=====================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: It is the main module of application. It lunches supervisor which then initializes appropriate components of node.
	:Behaviours: `application <http://www.erlang.org/doc/man/application.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`start/2 <veil_cluster_node_app;start/2>`
	* :ref:`stop/1 <veil_cluster_node_app;stop/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: veil_cluster_node_app

	.. _`veil_cluster_node_app;start/2`:

	.. erl:function:: start(_StartType :: any(), _StartArgs :: any()) -> Result

	* **Error:** {already_started, pid()} | {shutdown, term()} | term()
	* **Result:** {ok, pid()} | ignore | {error, Error}

	Starts application by supervisor initialization.

	.. _`veil_cluster_node_app;stop/1`:

	.. erl:function:: stop(_State :: any()) -> Result

	* **Result:** ok

	Stops application.

