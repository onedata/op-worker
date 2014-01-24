.. _veil_cluster_node_sup:

veil_cluster_node_sup
=====================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: It is the main supervisor. It starts (as it child) node manager which initializes node.
	:Behaviours: `supervisor <http://www.erlang.org/doc/man/supervisor.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`init/1 <veil_cluster_node_sup;init/1>`
	* :ref:`start_link/1 <veil_cluster_node_sup;start_link/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: veil_cluster_node_sup

	.. _`veil_cluster_node_sup;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **ChildSpec:** {Id :: term(), StartFunc, RestartPolicy, Type :: worker | supervisor, Modules}
	* **Modules:** [module()] | dynamic
	* **RestartPolicy:** permanent | transient | temporary
	* **RestartStrategy:** one_for_all | one_for_one | rest_for_one | simple_one_for_one
	* **Result:** {ok, {SupervisionPolicy, [ChildSpec]}} | ignore
	* **StartFunc:** {M :: module(), F :: atom(), A :: [term()] | undefined}
	* **SupervisionPolicy:** {RestartStrategy, MaxR :: non_neg_integer(), MaxT :: pos_integer()}

	 supervisor:init/1 

	.. _`veil_cluster_node_sup;start_link/1`:

	.. erl:function:: start_link(Args :: term()) -> Result

	* **Error:** {already_started, pid()} | {shutdown, term()} | term()
	* **Result:** {ok, pid()} | ignore | {error, Error}

	Starts application supervisor

