.. _dao_helper:

dao_helper
==========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: Low level BigCouch DB API

Function Index
~~~~~~~~~~~~~~~

	* :ref:`create_db/1 <dao_helper:create_db/1>`
	* :ref:`create_db/2 <dao_helper:create_db/2>`
	* :ref:`create_view/6 <dao_helper:create_view/6>`
	* :ref:`delete_db/1 <dao_helper:delete_db/1>`
	* :ref:`delete_db/2 <dao_helper:delete_db/2>`
	* :ref:`delete_doc/2 <dao_helper:delete_doc/2>`
	* :ref:`delete_docs/2 <dao_helper:delete_docs/2>`
	* :ref:`gen_uuid/0 <dao_helper:gen_uuid/0>`
	* :ref:`get_db_info/1 <dao_helper:get_db_info/1>`
	* :ref:`get_doc_count/1 <dao_helper:get_doc_count/1>`
	* :ref:`insert_doc/2 <dao_helper:insert_doc/2>`
	* :ref:`insert_doc/3 <dao_helper:insert_doc/3>`
	* :ref:`insert_docs/2 <dao_helper:insert_docs/2>`
	* :ref:`insert_docs/3 <dao_helper:insert_docs/3>`
	* :ref:`list_dbs/0 <dao_helper:list_dbs/0>`
	* :ref:`list_dbs/1 <dao_helper:list_dbs/1>`
	* :ref:`name/1 <dao_helper:name/1>`
	* :ref:`open_design_doc/2 <dao_helper:open_design_doc/2>`
	* :ref:`open_doc/2 <dao_helper:open_doc/2>`
	* :ref:`open_doc/3 <dao_helper:open_doc/3>`
	* :ref:`query_view/3 <dao_helper:query_view/3>`
	* :ref:`query_view/4 <dao_helper:query_view/4>`
	* :ref:`revision/1 <dao_helper:revision/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dao_helper:create_db/1`:

	.. function:: create_db(DbName :: string()) -> ok | {error, term()}
		:noindex:

	Creates db named DbName. If db already does nothing and returns 'ok'

	.. _`dao_helper:create_db/2`:

	.. function:: create_db(DbName :: string(), Opts :: [Option]) -> ok | {error, term()}
		:noindex:

	* **Option:** atom() | {atom(), term()}

	Creates db named DbName with Opts. Options can include values for q and n, for example {q, "8"} and {n, "3"}, which control how many shards to split a database into and how many nodes each doc is copied to respectively.

	.. _`dao_helper:create_view/6`:

	.. function:: create_view(DbName :: string(), DesignName :: string(), ViewName :: string(), Map :: string(), Reduce :: string(), DesignVersion :: integer()) -> [ok | {error, term()}]
		:noindex:

	Creates view with given Map and Reduce function. When Reduce = "", reduce function won't be created

	.. _`dao_helper:delete_db/1`:

	.. function:: delete_db(DbName :: string()) -> ok | {error, database_does_not_exist} | {error, term()}
		:noindex:

	Deletes db named DbName

	.. _`dao_helper:delete_db/2`:

	.. function:: delete_db(DbName :: string(), Opts :: [Option]) -> ok | {error, database_does_not_exist} | {error, term()}
		:noindex:

	* **Option:** atom() | {atom(), term()}

	Deletes db named DbName

	.. _`dao_helper:delete_doc/2`:

	.. function:: delete_doc(DbName :: string(), DocID :: string()) -> ok | {error, missing} | {error, deleted} | {error, term()}
		:noindex:

	Deletes doc from db

	.. _`dao_helper:delete_docs/2`:

	.. function:: delete_docs(DbName :: string(), [DocID :: string()]) -> [ok | {error, term()}]
		:noindex:

	Deletes list of docs from db

	.. _`dao_helper:gen_uuid/0`:

	.. function:: gen_uuid() -> string()
		:noindex:

	Generates UUID with CouchDBs 'utc_random' algorithm

	.. _`dao_helper:get_db_info/1`:

	.. function:: get_db_info(DbName :: string()) -> {ok, [ {instance_start_time, binary()} | {doc_count, non_neg_integer()} | {doc_del_count, non_neg_integer()} | {purge_seq, non_neg_integer()} | {compact_running, boolean()} | {disk_size, non_neg_integer()} | {disk_format_version, pos_integer()} ]} | {error, database_does_not_exist} | {error, term()}
		:noindex:

	Returns db info for the given DbName

	.. _`dao_helper:get_doc_count/1`:

	.. function:: get_doc_count(DbName :: string()) -> {ok, non_neg_integer()} | {error, database_does_not_exist} | {error, term()}
		:noindex:

	Returns doc count for given DbName

	.. _`dao_helper:insert_doc/2`:

	.. function:: insert_doc(DbName :: string(), Doc :: #doc{}) -> {ok, {RevNum :: non_neg_integer(), RevBin :: binary()}} | {error, conflict} | {error, term()}
		:noindex:

	Inserts doc to db

	.. _`dao_helper:insert_doc/3`:

	.. function:: insert_doc(DbName :: string(), Doc :: #doc{}, Opts :: [Option]) -> {ok, {RevNum :: non_neg_integer(), RevBin :: binary()}} | {error, conflict} | {error, term()}
		:noindex:

	* **Option:** atom() | {atom(), term()}

	Inserts doc to db

	.. _`dao_helper:insert_docs/2`:

	.. function:: insert_docs(DbName :: string(), [Doc :: #doc{}]) -> {ok, term()} | {error, term()}
		:noindex:

	Inserts list of docs to db

	.. _`dao_helper:insert_docs/3`:

	.. function:: insert_docs(DbName :: string(), [Doc :: #doc{}], Opts :: [Option]) -> {ok, term()} | {error, term()}
		:noindex:

	* **Option:** atom() | {atom(), term()}

	Inserts list of docs to db

	.. _`dao_helper:list_dbs/0`:

	.. function:: list_dbs() -> {ok, [string()]} | {error, term()}
		:noindex:

	Lists all dbs

	.. _`dao_helper:list_dbs/1`:

	.. function:: list_dbs(Prefix :: string()) -> {ok, [string()]} | {error, term()}
		:noindex:

	Lists all dbs that starts with Prefix

	.. _`dao_helper:name/1`:

	.. function:: name(Name :: string() | atom()) -> binary()
		:noindex:

	Converts string/atom to binary

	.. _`dao_helper:open_design_doc/2`:

	.. function:: open_design_doc(DbName :: string(), DesignName :: string()) -> {ok, #doc{}} | {error, {not_found, missing | deleted}} | {error, term()}
		:noindex:

	Returns design document with a given design doc name

	.. _`dao_helper:open_doc/2`:

	.. function:: open_doc(DbName :: string(), DocID :: string()) -> {ok, #doc{}} | {error, {not_found, missing | deleted}} | {error, term()}
		:noindex:

	Returns document with a given DocID

	.. _`dao_helper:open_doc/3`:

	.. function:: open_doc(DbName :: string(), DocID :: string(), Opts :: [Option]) -> {ok, #doc{}} | {error, {not_found, missing | deleted}} | {error, term()}
		:noindex:

	* **Option:** atom() | {atom(), term()}

	Returns document with a given DocID

	.. _`dao_helper:query_view/3`:

	.. function:: query_view(DbName :: string(), DesignName :: string(), ViewName :: string()) -> {ok, QueryResult :: term()} | {error, term()}
		:noindex:

	Execute a given view with default set of arguments. Check record #view_query_args for details.

	.. _`dao_helper:query_view/4`:

	.. function:: query_view(DbName :: string(), DesignName :: string(), ViewName :: string(), QueryArgs :: #view_query_args{}) -> {ok, QueryResult :: term()} | {error, term()}
		:noindex:

	Execute a given view. There are many additional query args that can be passed to a view, see <a href="http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options"> query args</a> for details.

	.. _`dao_helper:revision/1`:

	.. function:: revision(RevInfo :: term()) -> term()
		:noindex:

	Normalize revision info

