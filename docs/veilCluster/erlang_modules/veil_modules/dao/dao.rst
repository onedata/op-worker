.. _dao:

dao
===

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements :ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callbacks and contains utility API methods. DAO API functions are implemented in DAO sub-modules like: :ref:`dao_cluster <dao_cluster>`, :ref:`dao_vfs <dao_vfs>`. All DAO API functions Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead. Module :: atom() is module suffix (prefix is 'dao_'), MethodName :: atom() is the method name and ListOfArgs :: [term()] is list of argument for the method. If you want to call utility methods from this module - use Module = utils See :ref:`dao:handle/2 <dao:handle/2>` for more details.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <dao:cleanup/0>`
	* :ref:`doc_to_term/1 <dao:doc_to_term/1>`
	* :ref:`get_record/1 <dao:get_record/1>`
	* :ref:`handle/2 <dao:handle/2>`
	* :ref:`init/1 <dao:init/1>`
	* :ref:`init_storage/0 <dao:init_storage/0>`
	* :ref:`list_records/2 <dao:list_records/2>`
	* :ref:`load_view_def/2 <dao:load_view_def/2>`
	* :ref:`remove_record/1 <dao:remove_record/1>`
	* :ref:`save_record/1 <dao:save_record/1>`
	* :ref:`set_db/1 <dao:set_db/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dao:cleanup/0`:

	.. function:: cleanup() -> Result
		:noindex:

	* **Error:** timeout | term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback cleanup/0

	.. _`dao:doc_to_term/1`:

	.. function:: doc_to_term(Field :: term()) -> term()
		:noindex:

	Converts given valid BigCouch document body into erlang term(). If document contains saved record which is a valid record (see is_valid_record/1), then structure of the returned record will be updated

	.. _`dao:get_record/1`:

	.. function:: get_record(Id :: atom() | string()) -> {ok,#veil_document{record :: tuple()}} | {error, Error :: term()} | no_return()
		:noindex:

	Retrieves record with UUID = Id from DB. Returns whole #veil_document record containing UUID, Revision Info and demanded record inside. #veil_document{}.uuid and #veil_document{}.rev_info should not be ever changed. You can strip wrappers if you do not need them using API functions of dao_lib module. See #veil_document{} structure for more info. Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead.

	.. _`dao:handle/2`:

	.. function:: handle(ProtocolVersion :: term(), Request) -> Result
		:noindex:

	* **Args:** list()
	* **Error:** term()
	* **Method:** atom()
	* **Request:** {Method, Args} | {Mod :: atom(), Method, Args} | ping | get_version
	* **Response:** term()
	* **Result:** ok | {ok, Response} | {error, Error} | pong | Version
	* **Version:** term()

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback handle/1. All {Module, Method, Args} requests (second argument), executes Method with Args in {@type dao_Module} module, but with one exception: If Module = utils, then dao module will be used. E.g calling dao:handle(_, {vfs, some_method, [some_arg]}) will call dao_vfs:some_method(some_arg) but calling dao:handle(_, {utils, some_method, [some_arg]}) will call dao:some_method(some_arg) You can omit Module atom in order to use default module which is dao_cluster. E.g calling dao:handle(_, {some_method, [some_arg]}) will call dao_cluster:some_method(some_arg) Additionally all exceptions from called API method will be caught and converted into {error, Exception} tuple. E.g. calling handle(_, {save_record, [Id, Rec]}) will execute dao_cluster:save_record(Id, Rec) and normalize return value.

	.. _`dao:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Error:** term()
	* **Result:** ok | {error, Error}

	:ref:`worker_plugin_behaviour <worker_plugin_behaviour>` callback init/1

	.. _`dao:init_storage/0`:

	.. function:: init_storage() -> ok | {error, Error :: term()}
		:noindex:

	Inserts storage defined during worker instalation to database (if db already has defined storage, the function only replaces StorageConfigFile with that definition)

	.. _`dao:list_records/2`:

	.. function:: list_records(ViewInfo :: #view_info{}, QueryArgs :: #view_query_args{}) -> {ok, QueryResult :: #view_result{}} | {error, term()}
		:noindex:

	Executes view query and parses returned result into #view_result{} record. Strings from #view_query_args{} are not transformed by :ref:`dao_helper:name/1 <dao_helper:name/1>`, the caller has to do it by himself.

	.. _`dao:load_view_def/2`:

	.. function:: load_view_def(Name :: string(), Type :: map | reduce) -> string()
		:noindex:

	Loads view definition from file.

	.. _`dao:remove_record/1`:

	.. function:: remove_record(Id :: atom() | uuid()) -> ok | {error, Error :: term()}
		:noindex:

	Removes record with given UUID from DB Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead.

	.. _`dao:save_record/1`:

	.. function:: save_record(term() | #veil_document{uuid :: string(), rev_info :: term(), record :: term(), force_update :: boolean()}) -> {ok, DocId :: string()} | {error, conflict} | no_return()
		:noindex:

	Saves record to DB. Argument has to be either Record :: term() which will be saved with random UUID as completely new document or #veil_document record. If #veil_document record is passed caller may set UUID and revision info in order to update this record in DB. If you got #veil_document{} via :ref:`dao:get_record/1 <dao:get_record/1>`, uuid and rev_info are in place and you shouldn't touch them Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead.

	.. _`dao:set_db/1`:

	.. function:: set_db(DbName :: string()) -> ok
		:noindex:

	Sets current working database name

