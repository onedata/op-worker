%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @todo: write me!
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
-include_lib("dao/include/couch_db.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-define(dbsync_state, dbsync_state).

%% API
-export([init/1, handle/2, cleanup/0, changes_callback/3]).

-define(call_self(Req), gen_server:call(?Dispatcher_Name, {dbsync, 1, Req})).

-record(shard_info, {node, hostname = "", shard_name = ""}).


changes_callback(continuous, Changes, Acc) ->
    ?info("Changes ~p ~p", [Changes, Acc]),
    {ok, Acc}.

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
    DBNodes = dao_lib:apply(dao_hosts, list, [], 1),
    Shards = lists:map(
        fun(NodeName) ->
            [_, HostName] = string:tokens(atom_to_list(NodeName), "@"),
            [#shard_info{node = NodeName, hostname =  HostName, shard_name = ShardName} || ShardName <- get_shards_to_watch({NodeName, HostName})]
        end, DBNodes),

    Shards1 = lists:flatten(Shards),

    ?info("Shards ~p", [Shards1]),

    [watch_shard(ShardInfo) || ShardInfo <- Shards1],

    Res = dao_lib:apply(dao_helper, changes, [?FILES_DB_NAME, {dbsync, changes_callback, [continuous]}, [], #changes_args{feed = "continuous", include_docs = true}], 1),
    ets:new(?dbsync_state, [public, named_table, set]),
    ChangesReceiver = spawn_link(fun() -> changes_receiver_loop({main_poll, []}) end),
    register(dbsync_changes_receiver, ChangesReceiver),
    spawn(fun() -> timer:sleep(timer:seconds(1)), {ibrowse_req_id, RequestId} = ibrowse:send_req("http://127.0.0.1:5984/files/_changes?feed=longpoll&since=0", [], get, [], [{stream_to, ChangesReceiver}]) end),
    ?info("dbsync initialized ~p", [Res]),
    [].


watch_shard(#shard_info{hostname = HostName, shard_name = ShardName} = SI, SeqNum) ->
    ReceiverPid = case ets:lookup(?dbsync_state, SI) of
        [{_, Pid}] -> Pid;
        _ ->
            spawn_link(fun() -> changes_receiver_loop({SI, []}) end)
    end,

    {ibrowse_req_id, _RequestId} = ibrowse:send_req("http://" ++ HostName ++ ":5986/" ++ ShardName ++ "/_changes?feed=longpoll&since=" ++ , [], get, [], [{stream_to, ChangesReceiver}]
    .

get_shards_to_watch({_NodeName, HostName}) ->
    {ok, "200", _, Data} = ibrowse:send_req("http://" ++ HostName ++ ":5986/_all_dbs", [], get, []),
    DBs = json_decode(Data),
    lists:filter(
        fun(DBName) ->
            lists:prefix("shards/", DBName) andalso
            (string:str(DBName, "/" ++ ?FILES_DB_NAME ++ ".") > 0)
        end, DBs).

json_decode(JSON) ->
    (mochijson2:decoder([{object_hook, fun({struct,L}) -> {L} end}]))(JSON).

changes_receiver_loop({StreamId, State}) ->
    NewState = receive
        {ibrowse_async_response, _RequestId, Data} ->
            ?call_self({changes_stream, StreamId, Data}),
            State;
        {ibrowse_async_response_end, _} ->
            case StreamId of
                main_poll -> State;
                _         -> exit(normal)
            end
    after 60 ->
        State
    end,
    changes_receiver_loop({StreamId, NewState}).

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
            ?error_stacktrace("DB Sync ~p: ~p", [Type, Reason]),
            {error, Reason}
    end.


handle2(_ProtocolVersion, {changes_stream, main_poll, Data}) ->
    ?info("Changes ========================> ~p", [Data]),
    {Decoded} = (mochijson2:decoder([{object_hook, fun({struct,L}) -> {L} end}]))(Data),
    {_, Results} = lists:keyfind(<<"results">>, 1, Decoded),
    {_, [SeqNum, SeqHash]} = lists:keyfind(<<"last_seq">>, 1, Decoded),
    ?info("Changes decoded =======================> ~p ~p ~p", [Results, SeqNum, "http://127.0.0.1:5984/files/_changes?feed=longpoll&since=" ++ integer_to_list(SeqNum)]),

    ChangesReceiver = whereis(dbsync_changes_receiver),
    {ibrowse_req_id, _RequestId} = ibrowse:send_req("http://127.0.0.1:5984/files/_changes?feed=longpoll&since=" ++ integer_to_list(SeqNum), [], get, [], [{stream_to, ChangesReceiver}]),
    ok;

handle2(_ProtocolVersion, {{event, doc_saved}, {DbName, #db_document{rev_info = 0} = Doc, {NewSeq, NewRev}, Opts}}) ->
    handle(_ProtocolVersion, {{event, doc_saved}, {DbName, Doc#db_document{rev_info = {0, []}}, {NewSeq, NewRev}, Opts}});
handle2(_ProtocolVersion, {{event, doc_saved}, {?FILES_DB_NAME = DbName, #db_document{rev_info = {_OldSeq, OldRevs}} = Doc, {NewSeq, NewRev}, Opts}}) ->
    NewDoc = Doc#db_document{rev_info = {NewSeq, [NewRev | OldRevs]}},
    ok = emit_document(DbName, NewDoc);

handle2(_ProtocolVersion, {{event, doc_requested}, {?FILES_DB_NAME = DbName, DocUUID, DocRev}}) ->
    Request = #reportdocversion{doc_uuid = utils:ensure_binary(DocUUID), doc_rev = utils:ensure_binary(DocRev), doc_db_name = utils:ensure_binary(DbName)},
    {ok, #space_info{providers = SyncWith} = SpaceInfo} = get_space_ctx(DbName, DocUUID),
    SyncWithSorted = lists:usort(SyncWith),
    ok = tree_broadcast(SpaceInfo, SyncWithSorted, Request, 3);
handle2(_ProtocolVersion, {{event, doc_requested}, {_, _DocUUID, _DocRev}}) ->
    ok;

handle2(_ProtocolVersion, {{event, view_queried}, {#view_info{db_name = ?FILES_DB_NAME} = ViewInfo, QueryArgs, UUIDs}}) ->
    BinUUIDs = [utils:ensure_binary(UUID) || UUID <- UUIDs],
    Request = #reportviewqueryresult{view_info = encode_doc(ViewInfo), query_args = encode_doc(QueryArgs), uuids = BinUUIDs},
    {ok, #space_info{providers = SyncWith} = SpaceInfo} = get_space_ctx(ViewInfo, QueryArgs),
    SyncWithSorted = lists:usort(SyncWith),
    ok = tree_broadcast(SpaceInfo, SyncWithSorted, Request, 3);

handle2(_ProtocolVersion, {{event, doc_saved}, {DbName, Doc, NewRew, Opts}}) ->
    %% ?info("OMG wrong DB ~p", [{DbName, Doc, NewRew, Opts}]),
    ok;

handle2(_ProtocolVersion, {reemit, #treebroadcast{ledge = LEdge, redge = REdge, space_id = SpaceId,
                            input = RequestData, message_type = DecoderName} = BaseRequest}) ->
    {ok, #space_info{providers = Providers} = SpaceInfo} = fslogic_objects:get_space({uuid, SpaceId}),
    DecoderMethod = decoder_method(DecoderName),
    Request = apply(dbsync_pb, DecoderMethod, [RequestData]),
    Providers1 = lists:usort(Providers),
    Providers2 = lists:dropwhile(fun(Elem) -> Elem =/= LEdge end, Providers1),
    Providers3 = lists:takewhile(fun(Elem) -> Elem =/= REdge end, Providers2),
    Providers4 = lists:usort([REdge | Providers3]),
    ok = tree_broadcast(SpaceInfo, Providers4, Request, BaseRequest, 3);

handle2(ProtocolVersion, #treebroadcast{input = RequestData, message_type = DecoderName} = BaseRequest) ->
    DecoderMethod = decoder_method(DecoderName),
    Request = apply(dbsync_pb, DecoderMethod, [RequestData]),
    ?call_self({reemit, BaseRequest}),
    case handle_broadcast(ProtocolVersion, Request, BaseRequest) of
        ok -> ok;
        reemit ->
            ?call_self({reemit, BaseRequest}),
            ok;
        {error, Reason} ->
            {error, Reason}
    end;
handle2(_ProtocolVersion, Request) ->
    ?info("Hello ~p", [Request]);

handle2(_ProtocolVersion, _Msg) ->
    ?warning("dbsync: wrong request: ~p", [_Msg]),
    wrong_request.

handle_broadcast(ProtocolVersion, #docupdated{document = DocData} = Request, BaseRequest) ->
    #db_document{uuid = DocUUID, rev_info = {_, [EmitedRev | _]}} = Doc = decode_doc(DocData),
    DocDbName = doc_to_db(Doc),
    {ok, #space_info{} = SpaceInfo} = get_space_ctx(DocDbName, Doc),
    case dao_lib:apply(dao_records, save_record, [DocDbName, Doc, [replicated_changes]], ProtocolVersion) of
        {ok, _} ->
            ?info("UPDATED ~p OMG !!!!", [DocUUID]),
            ok;
        {error, Reason2} ->
            ?error("=================> Error2 while replicating changes ~p", [Reason2]),
            {error, Reason2}

    end,
    case dao_lib:apply(dao_records, get_record, [DocDbName, DocUUID, []], ProtocolVersion) of
        {ok, #db_document{rev_info = {_Seq, [CurrentRev | _]}} = CurrentDoc} ->
            case utils:ensure_binary(CurrentRev) =:= utils:ensure_binary(EmitedRev) of
                true ->
                    reemit;
                false ->
                    ok = emit_document(DocDbName, CurrentDoc)
            end,
            ok;
        {error, Reason1} ->
            ?error("=================> Error1 while replicating changes ~p", [Reason1]),
            reemit
    end;
handle_broadcast(ProtocolVersion, #reportdocversion{doc_uuid = UUID, doc_rev = DocRev, doc_db_name = DbName} = Request, BaseRequest) ->
    case dao_lib:apply(dao_records, get_record, [DbName, utils:ensure_list(UUID), []], ProtocolVersion) of
        {ok, #db_document{rev_info = {_, [DocRev | _]}}} ->
            ok;
        {ok, #db_document{} = CurrentDoc} ->
            ?info("Rev missmatch detected, pushing newer revision..."),
            ok = emit_document(DbName, CurrentDoc);
        {error, Reason2} ->
            ?error("=================> Error2 while processing reportdocversion ~p", [Reason2]),
            {error, Reason2}

    end,
    reemit;

handle_broadcast(ProtocolVersion, #reportviewqueryresult{view_info = #view_info{db_name = DbName} = ViewInfo, query_args = QueryArgs, uuids = UUIDs} = Request, BaseRequest) ->
    case dao_lib:apply(dao_records, list_records, [ViewInfo, QueryArgs, [skip_sync_events]], ProtocolVersion) of
        {ok, #view_result{rows = Rows}} ->
            RemoteMissing = [utils:ensure_binary(UUID) || #view_row{id = UUID} <- Rows] -- UUIDs,
            MissingDocs = [dao_lib:apply(dao_records, get_record, [DbName, UUID, [skip_sync_events]], 1) || UUID <- RemoteMissing],
            [emit_document(DbName, Doc) || Doc <- MissingDocs],
            ok;
        {error, Reason2} ->
            ?error("=================> Error2 while processing reportdocversion ~p", [Reason2]),
            {error, Reason2}

    end,
    reemit.


emit_document(DbName, #db_document{} = Doc) ->
    {ok, #space_info{providers = SyncWith} = SpaceInfo} = get_space_ctx(DbName, Doc),
    SyncWithSorted = lists:usort(SyncWith),

    ok = push_doc_changes(SpaceInfo, Doc, SyncWithSorted),
    ok = push_doc_changes(SpaceInfo, Doc, SyncWithSorted).


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
    ok.

get_space_ctx(#view_info{db_name = DbName} = ?FILE_TREE_VIEW, #view_query_args{start_key = [[ParentUUID, _]]}) ->
    get_space_ctx(DbName, ParentUUID);
get_space_ctx(#view_info{}, #view_query_args{}) ->
    {error, unsupported_view};
get_space_ctx(DbName, #db_document{uuid = UUID} = Doc) ->
    case ets:lookup(?dbsync_state, {uuid_to_spaceid, UUID}) of
        [{{uuid_to_spaceid, UUID}, SpaceId}] ->
            {ok, SpaceId};
        [] ->
            case get_space_ctx2(Doc, []) of
                {ok, {UUIDs, SpaceInfo}} ->
                    lists:foreach(
                        fun(FUUID) ->
                            ets:insert(?dbsync_state, {{uuid_to_spaceid, FUUID}, SpaceInfo})
                        end, UUIDs),
                    {ok, SpaceInfo};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
get_space_ctx(DbName, UUID) ->
    {ok, Doc} = dao_lib:apply(dao_records, get_record, [DbName, UUID, [skip_sync_events]], 1),
    get_space_ctx(DbName, Doc).
get_space_ctx2(#db_document{uuid = "", record = #file{}}, []) ->
    {error, no_space};
get_space_ctx2(#db_document{uuid = UUID, record = #file{extensions = Exts, parent = Parent}} = Doc, UUIDs) ->
    case lists:keyfind(?file_space_info_extestion, 1, Exts) of
        {?file_space_info_extestion, #space_info{} = SpaceInfo} ->
            {ok, {UUIDs, SpaceInfo}};
        false ->
            {ok, ParentDoc} = dao_lib:apply(vfs, get_file, [{uuid, Parent}], 1),
            get_space_ctx2(ParentDoc, [UUID | UUIDs])
    end;
get_space_ctx2(#db_document{uuid = UUID, record = #file_meta{}}, UUIDs) ->
    {ok, #db_document{} = FileDoc} = dao_lib:apply(dao_vfs, file_by_meta_id, [UUID], 1),
    get_space_ctx2(FileDoc, UUIDs).


push_doc_changes(#space_info{} = _SpaceInfo, _Doc, []) ->
    ok;
push_doc_changes(#space_info{} = SpaceInfo, Doc, SyncWith) ->
    Request = #docupdated{document = encode_doc(Doc)},
    ok = tree_broadcast(SpaceInfo, SyncWith, Request, 3).

push_doc_changes(#space_info{} = SpaceInfo, Doc, SyncWith, BaseRequest) ->
    Request = #docupdated{document = encode_doc(Doc)},
    ok = tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, 3).


tree_broadcast(SpaceInfo, SyncWith, Request, Attempts) ->
    ReqEncoder = encoder_method(get_message_type(Request)),
    BaseRequest = #treebroadcast{input = <<"">>, message_type = a2l(ReqEncoder), excluded_providers = [], ledge = <<"">>, redge = <<"">>, depth = 0, space_id = <<"">>},
    tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts).
tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts) ->
    SyncWith1 = SyncWith -- [cluster_manager_lib:get_provider_id()],
    case SyncWith1 of
        [] -> ok;
        _  ->
            {LSync, RSync} = lists:split(crypto:rand_uniform(0, length(SyncWith1)), SyncWith1),
            ExclProviders = [cluster_manager_lib:get_provider_id() | BaseRequest#treebroadcast.excluded_providers],
            NewBaseRequest = BaseRequest#treebroadcast{excluded_providers = lists:usort(ExclProviders)},
            do_emit_tree_broadcast(SpaceInfo, LSync, Request, NewBaseRequest, Attempts),
            do_emit_tree_broadcast(SpaceInfo, RSync, Request, NewBaseRequest, Attempts)
    end.

do_emit_tree_broadcast(_SpaceInfo, [], _Request, _NewBaseRequest, _Attempts) ->
    ok;
do_emit_tree_broadcast(#space_info{space_id = SpaceId} = SpaceInfo, SyncWith, Request, #treebroadcast{depth = Depth} = BaseRequest, Attempts) when Attempts > 0 ->
    PushTo = lists:nth(crypto:rand_uniform(1, 1 + length(SyncWith)), SyncWith),
    [LEdge | _] = SyncWith,
    REdge = lists:last(SyncWith),
    ReqEncoder = encoder_method(get_message_type(Request)),
    RequestData = iolist_to_binary(apply(dbsync_pb, ReqEncoder, [Request])),
    SyncRequest = BaseRequest#treebroadcast{ledge = LEdge, redge = REdge, depth = Depth + 1,
                                            message_type = a2l(get_message_type(Request)), input = RequestData, space_id = utils:ensure_binary(SpaceId
        )},
    SyncRequestData = dbsync_pb:encode_treebroadcast(SyncRequest),
    MsgId = provider_proxy_con:get_msg_id(),


    {AnswerDecoderName, AnswerType} = {rtcore, atom},


    RTRequest = #rtrequest{answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = SyncRequestData, message_decoder_name = a2l(dbsync),
        message_id = MsgId, message_type = a2l(utils:record_type(SyncRequest)), module_name = a2l(dbsync), protocol_version = 1, synch = true},
    RTRequestData = iolist_to_binary(rtcore_pb:encode_rtrequest(RTRequest)),

    URL = get_provider_url(PushTo),
    Timeout = 1000,
    provider_proxy_con:send({URL, <<"oneprovider">>}, MsgId, RTRequestData),
    receive
        {response, MsgId, AnswerStatus, WorkerAnswer} ->
            provider_proxy_con:report_ack({URL, <<"oneprovider">>}),
            ?debug("Answer for inter-provider pull request: ~p ~p", [AnswerStatus, WorkerAnswer]),
            case AnswerStatus of
                ?VOK ->
                    #atom{value = RValue} = erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]),
                    case RValue of
                        ?VOK -> ok;
                        _ -> throw(RValue)
                    end;
                InvalidStatus ->
                    ?error("Cannot reroute message ~p due to invalid answer status: ~p", [get_message_type(SyncRequest), InvalidStatus]),
                    do_emit_tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts - 1)
            end
    after Timeout ->
        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
        do_emit_tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts - 1)
    end;
do_emit_tree_broadcast(_SpaceInfo, _SyncWith, _Request, _BaseRequest, 0) ->
    {error, 'todo_error_code'}.


get_provider_url(ProviderId) ->
    {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
    _URL = lists:nth(crypto:rand_uniform(1, length(URLs) + 1), URLs).

%% a2l/1
%% ====================================================================
%% @doc Converts given list/atom to atom.
%% @end
-spec a2l(AtomOrList :: atom() | list()) -> Result :: atom().
%% ====================================================================
a2l(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
a2l(List) when is_list(List) ->
    List.


%% encoder_method/1
%% ====================================================================
%% @doc Get name of protobuf's encoder method for given message type.
%% @end
-spec encoder_method(MType :: atom() | list()) -> EncoderName :: atom().
%% ====================================================================
encoder_method(MType) when is_atom(MType) ->
    encoder_method(atom_to_list(MType));
encoder_method(MType) when is_list(MType) ->
    list_to_atom("encode_" ++ MType).


%% decoder_method/1
%% ====================================================================
%% @doc Get name of protobuf's decoder method for given message type.
%% @end
-spec decoder_method(MType :: atom() | list()) -> DecoderName :: atom().
%% ====================================================================
decoder_method(MType) when is_atom(MType) ->
    decoder_method(atom_to_list(MType));
decoder_method(MType) when is_list(MType) ->
    list_to_atom("decode_" ++ MType).


%% pb_module/1
%% ====================================================================
%% @doc Get protobuf's decoder's module name for given decoder's name.
%% @end
-spec pb_module(ModuleName :: atom() | list()) -> PBModule :: atom().
%% ====================================================================
pb_module(ModuleName) ->
    list_to_atom(utils:ensure_list(ModuleName) ++ "_pb").


%% get_message_type/1
%% ====================================================================
%% @doc Get message type.
%% @end
-spec get_message_type(Msg :: tuple()) -> Type :: atom().
%% ====================================================================
get_message_type(Msg) when is_tuple(Msg) ->
    utils:record_type(Msg).


encode_doc(Doc) ->
    term_to_binary(Doc).
decode_doc(Doc) ->
    binary_to_term(Doc).


doc_to_db(#db_document{record = #file{}}) ->
    ?FILES_DB_NAME;
doc_to_db(#db_document{record = #file_meta{}}) ->
    ?FILES_DB_NAME.


