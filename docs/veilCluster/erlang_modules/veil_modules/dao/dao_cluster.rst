.. _dao_cluster:

dao_cluster
===========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module gives high level DB API which contain veil cluster specific utility methods. All DAO API functions should not be called directly. Call dao:handle(_, {cluster, MethodName, ListOfArgs) instead. See :ref:`dao:handle/2 <dao;handle/2>` for more details.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`clear_sessions/0 <dao_cluster;clear_sessions/0>`
	* :ref:`clear_state/0 <dao_cluster;clear_state/0>`
	* :ref:`clear_state/1 <dao_cluster;clear_state/1>`
	* :ref:`close_connection/1 <dao_cluster;close_connection/1>`
	* :ref:`close_fuse_session/1 <dao_cluster;close_fuse_session/1>`
	* :ref:`get_connection_info/1 <dao_cluster;get_connection_info/1>`
	* :ref:`get_fuse_session/1 <dao_cluster;get_fuse_session/1>`
	* :ref:`get_fuse_session/2 <dao_cluster;get_fuse_session/2>`
	* :ref:`get_state/0 <dao_cluster;get_state/0>`
	* :ref:`get_state/1 <dao_cluster;get_state/1>`
	* :ref:`list_connection_info/1 <dao_cluster;list_connection_info/1>`
	* :ref:`list_fuse_sessions/1 <dao_cluster;list_fuse_sessions/1>`
	* :ref:`remove_connection_info/1 <dao_cluster;remove_connection_info/1>`
	* :ref:`remove_fuse_session/1 <dao_cluster;remove_fuse_session/1>`
	* :ref:`save_connection_info/1 <dao_cluster;save_connection_info/1>`
	* :ref:`save_fuse_session/1 <dao_cluster;save_fuse_session/1>`
	* :ref:`save_state/1 <dao_cluster;save_state/1>`
	* :ref:`save_state/2 <dao_cluster;save_state/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dao_cluster

	.. _`dao_cluster;clear_sessions/0`:

	.. erl:function:: clear_sessions() -> ok | no_return()

	Cleanups old, unused sessions from DB. Each session which is expired is checked. If there is at least one active connection for the session, its expire time will be extended. Otherwise it will be deleted. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;clear_state/0`:

	.. erl:function:: clear_state() -> ok | no_return()

	Removes cluster state with Id = cluster_state Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;clear_state/1`:

	.. _`dao_cluster;close_connection/1`:

	.. erl:function:: close_connection(SessID :: uuid()) -> ok | no_return()

	Removes connection_info record with given SessID form DB and tries to close it. This method should not be used unless connection exists. Otherwise it will fail with exception error. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;close_fuse_session/1`:

	.. erl:function:: close_fuse_session(FuseId :: uuid()) -> ok | no_return()

	Removes fuse_session record with given FuseID form DB and cache. Also deletes all connections that belongs to this session and tries to close them. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;get_connection_info/1`:

	.. erl:function:: get_connection_info(SessID :: uuid()) -> {ok, #veil_document{}} | no_return()

	Gets connection_info record with given SessID form DB. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;get_fuse_session/1`:

	.. _`dao_cluster;get_fuse_session/2`:

	.. erl:function:: get_fuse_session(FuseId :: uuid(), {stale, update_before | ok}) -> {ok, #veil_document{}} | no_return()

	Gets fuse_session record with given FuseID form DB. Second argument shall be either {stale, update_before} (will update cache before getting value) or {stale, ok} which is default - get value from cache. Default behaviour can be also achieved by ommiting second argument. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;get_state/0`:

	.. erl:function:: get_state() -> {ok, term()} | {error, any()}

	Retrieves cluster state with ID = cluster_state from DB. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;get_state/1`:

	.. erl:function:: get_state(Id :: atom()) -> {ok, term()} | {error, any()}

	Retrieves cluster state with UUID = Id from DB. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;list_connection_info/1`:

	.. erl:function:: list_connection_info({by_session_id, SessID :: uuid()}) -> {ok, [#veil_document{}]} | {error, any()}

	Lists connection_info records using given select condition. Current implementeation supports fallowing selects: {by_session_id, SessID} - select all connections that belongs to session with ID - SessID Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;list_fuse_sessions/1`:

	.. erl:function:: list_fuse_sessions({by_valid_to, Time :: non_neg_integer()}) -> {ok, [#veil_document{}]} | {error, any()}

	Lists fuse_session records using given select condition. Current implementeation supports fallowing selects: {by_valid_to, Time} - select all records whose 'valid_to' field is >= Time Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;remove_connection_info/1`:

	.. erl:function:: remove_connection_info(SessID :: uuid()) -> ok | no_return()

	Removes connection_info record with given SessID form DB. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;remove_fuse_session/1`:

	.. erl:function:: remove_fuse_session(FuseId :: uuid()) -> ok | no_return()

	Removes fuse_session record with given FuseID form DB and cache. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;save_connection_info/1`:

	.. erl:function:: save_connection_info(#connection_info{} | #veil_document{}) -> {ok, Id :: string()} | no_return()

	Saves connection_info record to DB. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;save_fuse_session/1`:

	.. erl:function:: save_fuse_session(#fuse_session{} | #veil_document{}) -> {ok, Id :: string()} | no_return()

	Saves fuse_session record to DB. If #fuse_session.valid_to field is not valid (i.e. its value is less then current timestamp) it will be set to default value (specified in application config). Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;save_state/1`:

	.. erl:function:: save_state(Rec :: tuple()) -> {ok, Id :: string()} | no_return()

	Saves cluster state Rec to DB with ID = cluster_state. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

	.. _`dao_cluster;save_state/2`:

	.. erl:function:: save_state(Id :: atom(), Rec :: tuple()) -> {ok, Id :: string()} | no_return()

	Saves cluster state Rec to DB with ID = Id. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead.

