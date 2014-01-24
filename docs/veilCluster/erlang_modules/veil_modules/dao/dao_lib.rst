.. _dao_lib:

dao_lib
=======

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: DAO helper/utility functional methods. Those can be used in other modules bypassing worker_host and gen_server.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`apply/4 <dao_lib:apply/4>`
	* :ref:`apply/5 <dao_lib:apply/5>`
	* :ref:`strip_wrappers/1 <dao_lib:strip_wrappers/1>`
	* :ref:`wrap_record/1 <dao_lib:wrap_record/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dao_lib:apply/4`:

	.. function:: apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()}, Args :: [term()], ProtocolVersion :: number()) -> any() | {error, worker_not_found}
		:noindex:

	Same as apply/5 but with default Timeout

	.. _`dao_lib:apply/5`:

	.. function:: apply(Module :: module(), Method :: atom() | {synch, atom()} | {asynch, atom()}, Args :: [term()], ProtocolVersion :: number(), Timeout :: pos_integer()) -> any() | {error, worker_not_found} | {error, timeout}
		:noindex:

	Behaves similar to erlang:apply/3 but works only with DAO worker<br/>. Method calls are made through random gen_server. <br/> Method should be tuple {synch, Method} or {asynch, Method}<br/> but if its simple atom(), {synch, Method} is assumed<br/> Timeout argument defines how long should this method wait for response

	.. _`dao_lib:strip_wrappers/1`:

	.. function:: strip_wrappers(VeilDocOrList :: #veil_document{} | [#veil_document{}]) -> tuple() | [tuple()]
		:noindex:

	Strips #veil_document{} wrapper. Argument can be either an #veil_document{} or list of #veil_document{} <br/> Alternatively arguments can be passed as {ok, Arg} tuple. Its convenient because most DAO methods formats return value formatted that way<br/> If the argument cannot be converted (e.g. error tuple is passed), this method returns it unchanged. <br/> This method is designed for use as wrapper for "get_*"-like DAO methods. E.g. dao_lib:strip_wrappers(dao_vfs:get_file({absolute_path, "/foo/bar"})) See :ref:`dao:save_record/1 <dao:save_record/1>` and :ref:`dao:get_record/1 <dao:get_record/1>` for more details about #veil_document{} wrapper.<br/>

	.. _`dao_lib:wrap_record/1`:

	.. function:: wrap_record(Record :: tuple()) -> #veil_document{}
		:noindex:

	Wraps given erlang record with #veil_document{} wrapper. <br/> See :ref:`dao:save_record/1 <dao:save_record/1>` and :ref:`dao:get_record/1 <dao:get_record/1>` for more details about #veil_document{} wrapper.<br/>

