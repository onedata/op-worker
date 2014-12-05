%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains DBSync worker and its other active elements
%% @end
%% ===================================================================
-module(dbsync).
-author("Rafal Slota").
-behaviour(worker_plugin_behaviour).


-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("fuse_messages_pb.hrl").
-include("dbsync_pb.hrl").
-include("rtcore_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("oneprovider_modules/dao/dao_db_structure.hrl").
-include("oneprovider_modules/dao/dao_cluster.hrl").
-include_lib("dao/include/couch_db.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-define(dbsync_state, dbsync_state).
-define(dbs_to_sync, [?FILES_DB_NAME]).

%% API
-export([init/1, handle/2, cleanup/0]).

-define(dbsync_cast(Req), gen_server:call(request_dispatcher, {dbsync, 1, Req})).


%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
    register(dbsync_state, spawn_link(dbsync_state, state_loop, [])),
    register(dbsync_ping_service, spawn_link(fun ping_service_loop/0)),

    ets:new(?dbsync_state, [public, named_table, set]),
    catch dbsync_state:load(), %% Try to load state from DB

    lists:foreach(
        fun(DbName) ->
            ChangesReceiver = spawn_link(fun() -> changes_receiver_loop({DbName, []}) end),
            register(changes_receiver_name(DbName), ChangesReceiver),

            {ok, DBInfo} = dao_lib:apply(dao_helper, get_db_info, [DbName], 1),
            {_, RawSeqInfo} = lists:keyfind(update_seq, 1, DBInfo),
            SeqInfo = dbsync_utils:normalize_seq_info(RawSeqInfo),
            ets:insert(?dbsync_state, {{last_seq, DbName}, SeqInfo}),

            %% @todo: remove this delayed init
            spawn(fun() -> timer:sleep(timer:seconds(1)), ?dbsync_cast({changes_stream, DbName, eof}) end)
        end, ?dbs_to_sync),

    register_available_blocks_hook(),
    register_file_meta_hook(),

    [].


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% @end
-spec handle(ProtocolVersion :: term(), Request :: term()) -> Result when
    Result :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(ProtocolVersion, Request) ->
    try
        handle2(ProtocolVersion, Request)
    catch
        Type:Reason ->
            ?error_stacktrace("[ DBSync ~p ] ~p", [Type, Reason]),
            {error, Reason}
    end.


%% handle2/2
%% ====================================================================
%% @doc Main dbsync's handle method.
%% @end
-spec handle2(ProtocolVersion :: term(), Request :: term()) -> Result when
    Result :: term().
%% ====================================================================

%% Hooks
handle2(_ProtocolVersion, {register_hook, Fun}) ->
    dbsync_hooks:register(Fun);

handle2(_ProtocolVersion, {remove_hook, HookId}) ->
    dbsync_hooks:unregister(HookId);

%% Changes stream responses
handle2(_ProtocolVersion, {changes_stream, StreamId, eof}) ->
    [{_, SeqInfo}] = ets:lookup(?dbsync_state, {last_seq, StreamId}),
    SeqReq1 = dbsync_utils:seq_info_to_url(SeqInfo),

    ChangesReceiver = whereis(changes_receiver_name(StreamId)),
    case ets:lookup(?dbsync_state, {last_request_id, StreamId}) of
        [{_, {LastRequestId, _}}] ->
            catch ibrowse:stream_close(LastRequestId);
        _ -> ok
    end,

    {ibrowse_req_id, RequestId} = ibrowse:send_req(
        select_db_url() ++ "/" ++ utils:ensure_list(StreamId) ++ "/_changes?feed=longpoll&include_docs=true&limit=100&heartbeat=3000&since=" ++ SeqReq1,
        [], get, [], [{inactivity_timeout, infinity}, {stream_to, ChangesReceiver}]),
    ets:insert(?dbsync_state, {{last_request_id, StreamId}, {RequestId, SeqInfo}}),
    ok;

handle2(_ProtocolVersion, {changes_stream, StreamId, Data, SinceSeqInfo}) ->
    try
        ?debug("Received DB changes ~p", [Data]),
        {ChangedDocs, {_SeqNum, _SeqHash}} = dbsync_utils:changes_json_to_docs(Data),
        ChangedDocs1 = [ChangedDoc || {#db_document{}, _} = ChangedDoc <- ChangedDocs],

        ok = ?dbsync_cast({docs_updated, {StreamId, ChangedDocs1, SinceSeqInfo}})
    catch
        _:Reason ->
            ?error("Cannot decode 'changes' stream (id: ~p) due to: ~p", [StreamId, Reason]),
            {error, Reason}
    end;

%% Events from changes stream
handle2(_ProtocolVersion, {docs_updated, {DbName, DocsWithSeq, SinceSeqInfo}}) ->
    case lists:member(DbName, ?dbs_to_sync) of
        true ->
            ok = emit_documents(DbName, DocsWithSeq, SinceSeqInfo);
        false ->
            ok
    end;

%% Reemitting tree broadcast messages
handle2(_ProtocolVersion, {reemit, #treebroadcast{ledge = LEdge, redge = REdge, space_id = SpaceId,
                            input = RequestData, message_type = DecoderName} = BaseRequest}) ->
    {ok, #space_info{providers = Providers} = SpaceInfo} = fslogic_objects:get_space({uuid, SpaceId}),
    DecoderMethod = dbsync_protocol:decoder_method(DecoderName),
    Request = apply(dbsync_pb, DecoderMethod, [RequestData]),
    Providers1 = lists:usort(Providers),
    Providers2 = lists:dropwhile(fun(Elem) -> Elem =/= LEdge end, Providers1),
    Providers3 = lists:takewhile(fun(Elem) -> Elem =/= REdge end, Providers2),
    Providers4 = lists:usort([REdge | Providers3]),
    ok = dbsync_protocol:tree_broadcast(SpaceInfo, Providers4, Request, BaseRequest, 3);

%% Handle treebroadcast{} message
handle2(ProtocolVersion, #treebroadcast{input = RequestData, message_type = DecoderName, space_id = SpaceId0, request_id = ReqId} = BaseRequest) ->
    SpaceId = utils:ensure_binary(SpaceId0),
    Ignore = dbsync_state:call(fun(_State) ->
        case dbsync_state:get({request, ReqId}) of
            undefined ->
                dbsync_state:set({request, ReqId}, utils:mtime()),
                false;
            _MTime ->
                true
        end
    end),

    case Ignore of
        false ->

            DecoderMethod = dbsync_protocol:decoder_method(DecoderName),
            Request = apply(dbsync_pb, DecoderMethod, [RequestData]),

            case handle_broadcast(ProtocolVersion, SpaceId, Request, BaseRequest) of
                ok -> ok;
                reemit ->
                    case ?dbsync_cast({reemit, BaseRequest}) of
                        ok -> ok;
                        {error, Reason} ->
                            ?error("Cannot reemit tree broadcast due to: ~p", [Reason]),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ?error("Error while handling tree broadcast: ~p", [Reason]),
                    {error, Reason}
            end;
        true -> ok
    end;

%% Handle requestseqdiff{} message
handle2(_ProtocolVersion, #requestseqdiff{space_id = SpaceId, dbname = DbName, since_seq = SinceBin} = _BaseRequest) ->
    SinceSeqInfo = dbsync_utils:normalize_seq_info(dbsync_utils:decode_term(SinceBin)),
    SeqReq = dbsync_utils:seq_info_to_url(SinceSeqInfo),

    {ok, "200", _, Data} = ibrowse:send_req(select_db_url() ++ "/" ++ utils:ensure_list(DbName) ++ "/_changes?feed=normal&include_docs=true&since=" ++ SeqReq, [], get, []),
    {ChangedDocs, ReqSeqInfo} = dbsync_utils:changes_json_to_docs(Data),
    ChangedDocs1 = [ChangedDoc || {#db_document{}, _} = ChangedDoc <- ChangedDocs],

    SpacesMap = lists:foldl(
        fun({#db_document{uuid = UUID, rev_info = {RevNum, [RevHash | _]}} = Doc, CSeq}, Acc) ->
            try
                {ok, #space_info{providers = _SyncWith, space_id = SpaceId} = SpaceInfo} = get_space_ctx(DbName, Doc),
                {ok, [{ok, #doc{revs = RevInfo}}]} = dao_lib:apply(dao_helper, open_revs, [DbName, UUID, [{RevNum, RevHash}], []], 1),
                NewDoc = Doc#db_document{rev_info = RevInfo},

                {_, LastSSeq, SpaceDocs} = maps:get(SpaceId, Acc, {SpaceInfo, 0, []}),
                maps:put(SpaceId, {SpaceInfo, max(LastSSeq, CSeq), [NewDoc | SpaceDocs]}, Acc)
            catch
                _:{badmatch,{error,{not_found,missing}}} ->
                    Acc;
                _:{badmatch, {ok, #space_info{}}} ->
                    Acc;
                _:Reason ->
                    ?error_stacktrace("Unable to emit document ~p due to ~p", [Doc, Reason]),
                    Acc
            end
        end, #{}, ChangedDocs1),

    {_, _, Docs} = maps:get(SpaceId, SpacesMap, {#space_info{}, 0, []}),
    DocsEncoded = [dbsync_utils:encode_term(Doc) || Doc <- lists:reverse(Docs)],
    #docupdated{dbname = DbName, document =  DocsEncoded, prev_seq = SinceBin, curr_seq = dbsync_utils:encode_term(ReqSeqInfo)};

handle2(_ProtocolVersion, _Msg) ->
    ?warning("dbsync: unknown request: ~p", [_Msg]),
    unknown_request.


%% handle_broadcast/2
%% ====================================================================
%% @doc General handler for treebroadcast{} inner-messages. Shall return whether
%%      broadcast should be canceled (ok | {error, _}) or reemitted (reemit).
%% @end
-spec handle_broadcast(ProtocolVersion :: term(), SpaceId :: binary(), Request :: term(), BaseRequest :: term()) ->
    ok | reemit | {error, Reason :: any()}.
%% ====================================================================
handle_broadcast(_ProtocolVersion, SpaceId, #docupdated{dbname = DbName, document = DocsData, prev_seq = PrevSeqInfoBin, curr_seq = CurrSeqInfoBin} = Request, BaseRequest) ->
    {provider_id, ProviderId} = get(peer_id),
    CurrSeqInfo = dbsync_utils:decode_term(CurrSeqInfoBin),
    PrevSeqInfo = dbsync_utils:decode_term(PrevSeqInfoBin),
    Docs = lists:map(
        fun(DocData) ->
            #db_document{uuid = _DocUUID, rev_info = {_, [_EmitedRev | _]}} = Doc = dbsync_utils:decode_term(DocData)
        end, DocsData),

    {MetaDocs, OtherDocs} =
        lists:partition(fun
            (#db_document{record = #file_meta{}}) ->
                true;
            (_) ->
                false
        end, Docs),

    replicate_docs(SpaceId, OtherDocs),
    replicate_docs(SpaceId, MetaDocs),

    dbsync_state:call(fun(_State) ->
        case get_current_seq(ProviderId, SpaceId, DbName) of
            PrevSeqInfo ->
                ?debug("State of space ~p updated to ~p", [SpaceId, CurrSeqInfo]),
                ets:insert(?dbsync_state, {last_space_seq_key(ProviderId, SpaceId, DbName), CurrSeqInfo});
            UnknownSeq ->
                ?debug("Cannot update database with received diff due to seq missmatch ~p vs local ~p", [PrevSeqInfo, UnknownSeq])
        end
    end),
    reemit;

handle_broadcast(_ProtocolVersion, SpaceId, #reportspacestate{current_seq = Seqs} = _Request, _BaseRequest) ->
    SeqsInfo = [{DbName, dbsync_utils:decode_term(SeqNumBin)} || #reportspacestate_currentseqdata{dbname = DbName, seq_num = SeqNumBin} <- Seqs],
    {provider_id, ProviderId} = get(peer_id),
    lists:foreach(
        fun({DbName, SeqInfo}) ->
            case get_current_seq(ProviderId, SpaceId, DbName) of
                LocalSeq when LocalSeq >= SeqInfo -> ok;
                LocalSeq ->
                    ?debug("Seq missmatch for space ~p in database ~p for provider ~p: ~p vs local ~p", [SpaceId, DbName, ProviderId, SeqInfo, LocalSeq]),
                    spawn(fun() -> request_diff(ProviderId, SpaceId, DbName, LocalSeq, 3) end)
            end
        end, SeqsInfo),
    ?info("Got dbsync report for space ~p: ~p", [SpaceId, SeqsInfo]),
    reemit;


handle_broadcast(_ProtocolVersion, SpaceId, #requestspacestate{dbname = _DbName} = _Request, _BaseRequest) ->
    broadcast_space_state(SpaceId),
    reemit.



%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
    ok.


%% request_diff/5
%% ====================================================================
%% @doc Requests changes diff from given SinceSeqInfo.
%%      This method receives response and replicates all documents.
%% @end
-spec request_diff(ProviderId :: binary(), SpaceId :: binary(), DbName :: string() | binary(),
    SinceSeqInfo :: term(), Attempts :: non_neg_integer()) ->
    ok | no_return().
%% ====================================================================
request_diff(ProviderId, SpaceId, DbName, SinceSeqInfo, Attempts) ->
    Request = #requestseqdiff{space_id = SpaceId, dbname = utils:ensure_binary(DbName), since_seq = dbsync_utils:encode_term(SinceSeqInfo)},
    #docupdated{document = DocsData, curr_seq = CurrSeqInfoBin, prev_seq = PrevSeqInfoBin} = dbsync_protocol:send_direct_message(ProviderId, Request, {dbsync, docupdated}, Attempts),
    CurrSeqInfo = dbsync_utils:decode_term(CurrSeqInfoBin),
    PrevSeqInfo = dbsync_utils:decode_term(PrevSeqInfoBin),

    Docs = lists:map(
        fun(DocData) ->
            #db_document{uuid = _DocUUID, rev_info = {_, [_EmitedRev | _]}} = Doc = dbsync_utils:decode_term(DocData)
        end, DocsData),

    {MetaDocs, OtherDocs} =
        lists:partition(fun
            (#db_document{record = #file_meta{}}) ->
                true;
            (_) ->
                false
        end, Docs),

    replicate_docs(SpaceId, OtherDocs),
    replicate_docs(SpaceId, MetaDocs),

    dbsync_state:call(fun(_State) ->
        case get_current_seq(ProviderId, SpaceId, DbName) of
            PrevSeqInfo ->
                ?debug("State of space ~p updated to ~p!", [SpaceId, CurrSeqInfo]),
                ets:insert(?dbsync_state, {last_space_seq_key(ProviderId, SpaceId, DbName), CurrSeqInfo});
            UnknownSeq ->
                ?debug("Cannot update database with requested diff due to seq missmatch ~p vs local ~p", [PrevSeqInfo, UnknownSeq])
        end
    end),
    ok.


%% emit_documents/3
%% ====================================================================
%% @doc Emits given changes to all providers that supports spaces associated with those documents.
%% @end
-spec emit_documents(DbName :: string() | binary(), DocsWithSeq :: [{#db_document{}, SeqInfo :: term()}], SinceSeqInfo :: term()) ->
    ok | no_return().
%% ====================================================================
emit_documents(DbName, DocsWithSeq, SinceSeqInfo) ->
    Hooks =
        case dbsync_state:get(hooks) of
            undefined -> [];
            Hooks0 -> Hooks0
        end,

    SpacesMap = lists:foldl(
        fun({#db_document{uuid = UUID, rev_info = {RevNum, [RevHash | _]}} = Doc, CSeq}, Acc) ->
            try
                {ok, #space_info{providers = SyncWith, space_id = SpaceId} = SpaceInfo} = get_space_ctx(DbName, Doc),
                SyncWithSorted = lists:usort(SyncWith),
                {ok, [{ok, #doc{revs = RevInfo}}]} = dao_lib:apply(dao_helper, open_revs, [DbName, UUID, [{RevNum, RevHash}], []], 1),
                NewDoc = Doc#db_document{rev_info = RevInfo},

                [spawn(fun() -> catch Callback(DbName, SpaceId, UUID, NewDoc) end) || {_, Callback} <- Hooks],

                {_, LastSSeq, SpaceDocs} = maps:get(SpaceId, Acc, {SpaceInfo, 0, []}),
                maps:put(SpaceId, {SpaceInfo, max(LastSSeq, CSeq), [NewDoc | SpaceDocs]}, Acc)
            catch
                _:{badmatch,{error,{not_found,missing}}} ->
                    Acc;
                _:Reason ->
                    ?error_stacktrace("Unable to emit document ~p due to ~p", [UUID, Reason]),
                    Acc
            end
        end, #{}, DocsWithSeq),

    lists:foreach(
        fun({SpaceId, {#space_info{providers = SyncWith} = SpaceInfo, LastSpaceSeq, Docs}}) ->
            Docs1 = lists:reverse(Docs),
            dbsync_state:call(fun(_) ->
                case ets:lookup(?dbsync_state, last_space_seq_key(SpaceId, DbName)) of
                    [{_, LastSeq}] when LastSeq < LastSpaceSeq ->
                        ets:insert(?dbsync_state, {last_space_seq_key(SpaceId, DbName), LastSpaceSeq}),
                        LastSeq;
                    [] ->
                        ets:insert(?dbsync_state, {last_space_seq_key(SpaceId, DbName), LastSpaceSeq}),
                        0;
                    [{_, LastSeq}] -> LastSpaceSeq
                end
            end),
            SyncWithSorted = lists:usort(SyncWith),
            ok = push_doc_changes(SpaceInfo, Docs1, SinceSeqInfo, LastSpaceSeq, SyncWithSorted),
            ok = push_doc_changes(SpaceInfo, Docs1, SinceSeqInfo, LastSpaceSeq, SyncWithSorted)
        end, maps:to_list(SpacesMap)).


%% push_doc_changes/3
%% ====================================================================
%% @doc Emits given documents from specific space to all providers that support this space.
%%      SinceSeqInfo is the lower bound of changes, while LastSpaceSeq is the upper bound.
%%      SyncWith has to be sorted.
%% @end
-spec push_doc_changes(#space_info{}, Docs :: [#db_document{}], SinceSeqInfo :: term(), LastSpaceSeq :: term(), SyncWith :: [binary()]) ->
    ok | no_return().
%% ====================================================================
push_doc_changes(#space_info{} = _SpaceInfo, _Docs, _, _, []) ->
    ok;
push_doc_changes(#space_info{} = _SpaceInfo, [], _, _, _) ->
    ok;
push_doc_changes(#space_info{} = SpaceInfo, Docs, SinceSeqInfo, LastSpaceSeq, SyncWith) ->
    [SomeDoc | _] = Docs,
    Request = #docupdated{dbname = utils:ensure_binary(dbsync_records:doc_to_db(SomeDoc)), document = [dbsync_utils:encode_term(Doc) || Doc <- Docs], curr_seq = dbsync_utils:encode_term(LastSpaceSeq), prev_seq = dbsync_utils:encode_term(SinceSeqInfo)},
    ok = dbsync_protocol:tree_broadcast(SpaceInfo, SyncWith, Request, 3).


%% broadcast_space_state/1
%% ====================================================================
%% @doc Broadcasts current sequence numbers for given space to other providers.
%% @end
-spec broadcast_space_state(SpaceId :: binary()) ->
    ok | no_return().
%% ====================================================================
broadcast_space_state(SpaceId) ->
    {ok, #space_info{providers = SyncWith} = SpaceInfo} = fslogic_objects:get_space({uuid, SpaceId}),
    SeqData =
        lists:map(
            fun(DbName) ->
                SeqInfo =
                    case ets:lookup(?dbsync_state, last_space_seq_key(SpaceId, DbName)) of
                        [{_, SeqInfo1}] ->
                            SeqInfo1;
                        _ ->
                            [{_, SeqInfo2}] = ets:lookup(?dbsync_state, {last_seq, DbName}),
                            SeqInfo2
                    end,
                #reportspacestate_currentseqdata{dbname = utils:ensure_binary(DbName), seq_num = dbsync_utils:encode_term(SeqInfo)}
            end, ?dbs_to_sync),

    Request = #reportspacestate{current_seq = SeqData},
    dbsync_protocol:tree_broadcast(SpaceInfo, lists:usort(SyncWith), Request, 3).


%% ====================================================================
%% Names, URLs, etc.
%% ====================================================================


%% select_db_url/0
%% ====================================================================
%% @doc Gets DB's URL (currently chosen randomly from all available).
%% @end
-spec select_db_url() -> string().
%% ====================================================================
select_db_url() ->
    HostNames1 =
        case dbsync_state:get(db_hosts) of
            undefined ->
                {ok, NodeNames} = dao_lib:apply(dao_hosts, list, [], 1),
                HostNames = lists:map(fun(NodeName) ->
                    [_, HostName] = string:tokens(atom_to_list(NodeName), "@"),
                    HostName
                end, NodeNames),
                dbsync_state:set(db_hosts, HostNames),
                HostNames;
            HostNames0 ->
                HostNames0
        end,
    HostName = lists:nth(crypto:rand_uniform(0, length(HostNames1)) + 1, HostNames1),
    "http://" ++ HostName ++ ":5984".


%% changes_receiver_name/1
%% ====================================================================
%% @doc Generates receiver's process name for given stream.
%% @end
-spec changes_receiver_name(StreamId :: binary() | string()) ->
    atom().
%% ====================================================================
changes_receiver_name(StreamId) ->
    list_to_atom("changes_receiver_" ++ utils:ensure_list(StreamId)).



%% last_space_seq_key/2
%% ====================================================================
%% @doc Generates state's key for setting/getting latest sequence number for given: SpaceId, DbName and local provider.
%% @end
-spec last_space_seq_key(SpaceId :: binary(), DbName :: binary() | string()) ->
    term().
%% ====================================================================
last_space_seq_key(SpaceId, DbName) ->
    last_space_seq_key(cluster_manager_lib:get_provider_id(), SpaceId, DbName).


%% last_space_seq_key/3
%% ====================================================================
%% @doc Generates state's key for setting/getting latest sequence number for given: ProviderId, SpaceId, DbName.
%% @end
-spec last_space_seq_key(ProviderId :: binary(), SpaceId :: binary(), DbName :: binary() | string()) ->
    term().
%% ====================================================================
last_space_seq_key(ProviderId, SpaceId, DbName) ->
    {last_space_seq, ProviderId, utils:ensure_binary(SpaceId), utils:ensure_binary(DbName)}.


%% ====================================================================
%% Active elements
%% ====================================================================

%% changes_receiver_loop/1
%% ====================================================================
%% @doc Endless loop that receives data from DB REST API (_changes) and passes to dbsync worker.
%%      There is one receiver per _changes stream (i.e. per database/bucket)
%% @end
-spec changes_receiver_loop({StreamId :: term(), State :: term()}) -> no_return().
%% ====================================================================
changes_receiver_loop({StreamId, State}) ->
    NewState = receive
                   {ibrowse_async_response, RequestId, Data} ->
                       try
                           [{_, {_, SinceSeqInfo}}] = ets:lookup(?dbsync_state, {last_request_id, StreamId}),
                           {Decoded} = dbsync_utils:json_decode(Data),
                           {_, [SeqNum, SeqHash]} = lists:keyfind(<<"last_seq">>, 1, Decoded),
                           ok = ?dbsync_cast({changes_stream, StreamId, Data, SinceSeqInfo}),
                           ets:insert(?dbsync_state, {{last_seq, StreamId}, {SeqNum, SeqHash}})
                       catch
                           Type:Reason ->
                               ?warning_stacktrace("Cannot decode 'changes' stream or dbsync worker is not available (~p: ~p)", [Type, Reason]),
                               ok
                       end,

                       State;
                   {ibrowse_async_response_end, _} ->
                       ?dbsync_cast({changes_stream, StreamId, eof}),
                       State;
                   {ibrowse_async_headers, _RequestId, "200", _} ->
                       State;
                   Unk ->
                       ?error("Unknown response from changes stream: ~p", [Unk]),
                       ?dbsync_cast({changes_stream, StreamId, eof}),
                       State
               after timer:seconds(10) ->
                   ?dbsync_cast({changes_stream, StreamId, eof}),
                   State
               end,
    changes_receiver_loop({StreamId, NewState}).


%% ping_service_loop/0
%% ====================================================================
%% @doc Endless loop that broadcasts current sequence numbers to other providers.
%% @end
-spec ping_service_loop() -> no_return().
%% ====================================================================
ping_service_loop() ->
    timer:sleep(timer:seconds(5)),
    try
        {ok, SpaceIds} = gr_providers:get_spaces(provider),
        lists:foreach(
            fun(SpaceId) ->
                lists:foreach(
                    fun(DbName) ->
                        Request = #requestspacestate{dbname = utils:ensure_binary(DbName)},
                        {ok, #space_info{providers = SyncWith} = SpaceInfo} = fslogic_objects:get_space({uuid, SpaceId}),
                        ok = dbsync_protocol:tree_broadcast(SpaceInfo, lists:usort(SyncWith), Request, 3)
                    end, ?dbs_to_sync)
            end, SpaceIds)
    catch
        Type:Reason ->
            ?error_stacktrace("Cannot request space sync status due to ~p: ~p", [Type, Reason]),
            {error, {Type, Reason}}
    end,

    ping_service_loop([]).
ping_service_loop(Spaces) ->
    NewSpaces =
    %% Update supported spaces list if possible
        case gr_providers:get_spaces(provider) of
            {ok, SpaceIds} ->
                SpaceIds;
            {error, _Reason} ->
                Spaces
        end,
    Res = [catch broadcast_space_state(SpaceId) || SpaceId <- NewSpaces],
    ?debug("DBSync ping result: ~p", [Res]),

    catch dbsync_state:clear_requests(),
    catch dbsync_state:save(),

    timer:sleep(timer:seconds(30)),
    ping_service_loop(NewSpaces).


%% ====================================================================
%% Misc
%% ====================================================================

%% get_space_ctx/2
%% ====================================================================
%% @doc For given docuemnt, returns associated #space_info{}. Uses internal cache
%%      and dbsync_records:get_space_ctx/2 for getting new data.
%% @end
-spec get_space_ctx(DbName :: string() | binary(), #db_document{}) ->
    {ok, #space_info{}} | {error, Reason :: any()}.
%% ====================================================================
get_space_ctx(_DbName, #db_document{uuid = UUID} = Doc) ->
    case dbsync_state:get({uuid_to_spaceid, UUID}) of
        undefined ->
            case dbsync_records:get_space_ctx(Doc, []) of
                {ok, {UUIDs, SpaceInfo}} ->
                    lists:foreach(
                        fun(FUUID) ->
                            dbsync_state:set({uuid_to_spaceid, FUUID}, SpaceInfo)
                        end, UUIDs),
                    {ok, SpaceInfo};
                {error, Reason} ->
                    ?warning("Cannot get space info for document ~p due to ~p", [UUID, Reason]),
                    {error, Reason}
            end;
        SpaceId ->
            {ok, SpaceId}
    end;
get_space_ctx(DbName, UUID) ->
    {ok, Doc} = dao_lib:apply(dao_records, get_record, [DbName, UUID, []], 1),
    get_space_ctx(DbName, Doc).


%% get_current_seq/2
%% ====================================================================
%% @doc Gets current sequence number for given {ProviderId, SpaceId, DbName}.
%%      For local provider it will be newest sequence number from database,
%%      for others - newest synchronized sequence number.
%% @end
-spec get_current_seq(ProviderId :: binary(), SpaceId :: binary(), DbName :: binary() | string()) ->
    {SeqNum :: non_neg_integer(), SeqHash :: binary()}.
%% ====================================================================
get_current_seq(ProviderId, SpaceId, DbName) ->
    case dbsync_state:get(last_space_seq_key(ProviderId, SpaceId, DbName)) of
        undefined ->
            {0, <<>>};
        CurrSeqInfo -> CurrSeqInfo
    end.


replicate_docs(_SpaceId, []) ->
    ok;
replicate_docs(SpaceId, [#db_document{uuid = DocUUID} = Doc | T]) ->
    try replicate_doc(SpaceId, Doc) of
        ok -> ok;
        {error, Reason} ->
            ?error("Cannot replicate document ~p due to ~p", [DocUUID, Reason])
    catch
        Type1:Reason1 ->
            ?error("Cannot replicate document ~p due to ~p", [DocUUID, {Type1, Reason1}])
    end,
    replicate_docs(SpaceId, T).


%% replicate_doc/2
%% ====================================================================
%% @doc Replicates given document to DB.
%% @end
-spec replicate_doc(SpaceId :: binary(), #db_document{}) -> ok | {error, Reason :: any()}.
%% ====================================================================
replicate_doc(SpaceId, #db_document{uuid = DocUUID} = Doc) ->
    Hooks =
        case dbsync_state:get(hooks) of
            undefined -> [];
            Hooks0 -> Hooks0
        end,

    DocDbName = dbsync_records:doc_to_db(Doc),
    %% {ok, #space_info{space_id = SpaceId} = _SpaceInfo} = get_space_ctx(DocDbName, Doc),
    try get_space_ctx(DocDbName, Doc) of
        {ok, #space_info{space_id = SpaceId}} ->
            ?debug("Replication of document ~p verified", [DocUUID]);
        {ok, #space_info{space_id = RealSpaceId}} ->
            ?warning("Invalid space for document ~p. Expected ~p, got ~p", [DocUUID, RealSpaceId, SpaceId]);
        {error, Reason3} ->
            ?warning("Unable to get space for document ~p due to ~p", [DocUUID, Reason3])
    catch
        _:Reason4 ->
            ?warning("Unable to get space for document ~p due to ~p", [DocUUID, Reason4])
    end,

    case dao_lib:apply(dao_records, save_record, [DocDbName, Doc, [replicated_changes]], 1) of
        {ok, _} ->
            [spawn(fun() -> catch Callback(DocDbName, SpaceId, DocUUID, Doc) end) || {_, Callback} <- Hooks],
            ?debug("Document ~p replicated", [DocUUID]),
            ok;
        {error, Reason2} ->
            ?error("Cannot replicate changes due to: ~p", [Reason2]),
            {error, Reason2}

    end.


register_file_meta_hook() ->
    HookFun = fun
        (?FILES_DB_NAME, _, FileUUID, #db_document{record = #file_meta{}} = Doc)->
            fslogic_events:on_file_meta_update(FileUUID, Doc);
        (_, _, _, _) -> ok
    end,

    {ok, Delay} = application:get_env(?APP_Name, dbsync_hook_registering_delay),
    erlang:send_after(Delay, self(), {timer, {asynch, 1, {register_hook, HookFun}}}).

register_available_blocks_hook() ->
    % register hook for #available_blocks docs
    HookFun = fslogic_available_blocks:db_sync_hook(),

    {ok, Delay} = application:get_env(?APP_Name, dbsync_hook_registering_delay),
    erlang:send_after(Delay, self(), {timer, {asynch, 1, {register_hook, HookFun}}}).
