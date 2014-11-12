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
-include("oneprovider_modules/dao/dao_cluster.hrl").
-include_lib("dao/include/couch_db.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-define(dbsync_state, dbsync_state).
-define(dbs_to_sync, [?FILES_DB_NAME]).

%% API
-export([init/1, handle/2, cleanup/0, state_loop/1, sync_call/1]).

-define(dbsync_cast(Req), gen_server:call(request_dispatcher, {dbsync, 1, Req})).


%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
    register(dbsync_state, spawn_link(fun state_loop/0)),
    register(dbsync_ping_service, spawn_link(fun ping_service_loop/0)),

    ets:new(?dbsync_state, [public, named_table, set]),
    catch state_load(), %% Try to load state from DB


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
        "http://127.0.0.1:5984/" ++ utils:ensure_list(StreamId) ++ "/_changes?feed=longpoll&include_docs=true&limit=100&heartbeat=3000&since=" ++ SeqReq1,
        [], get, [], [{inactivity_timeout, infinity}, {stream_to, ChangesReceiver}]),
    ets:insert(?dbsync_state, {{last_request_id, StreamId}, {RequestId, SeqInfo}}),
    ok;

handle2(_ProtocolVersion, {changes_stream, StreamId, Data, SinceSeqInfo}) ->
    try
        ?info("Changes ========================> ~p", [Data]),
        {Decoded} = dbsync_utils:json_decode(Data),
        {ChangedDocs, {_SeqNum, _SeqHash}} = dbsync_utils:changes_json_to_docs(Data),
        ChangedDocs1 = [ChangedDoc || {#db_document{}, _} = ChangedDoc <- ChangedDocs],

        ok = ?dbsync_cast({docs_updated, {StreamId, ChangedDocs1, SinceSeqInfo}})
    catch
        _:Reason ->
            ?error("Cannot decode 'changes' stream (id: ~p) due to: ~p", [StreamId, Reason]),
            {error, Reason}
    end;


handle2(_ProtocolVersion, {docs_updated, {DbName, DocsWithSeq, SinceSeqInfo}}) ->
    case lists:member(?FILES_DB_NAME, ?dbs_to_sync) of
        true ->
            ok = emit_documents(DbName, DocsWithSeq, SinceSeqInfo);
        false ->
            ok
    end;

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

handle2(ProtocolVersion, #treebroadcast{input = RequestData, message_type = DecoderName, space_id = SpaceId, request_id = ReqId} = BaseRequest) ->

    Ignore = sync_call(fun(_State) ->
        case state_get({request, ReqId}) of
            undefined ->
                state_set({request, ReqId}, utils:mtime()),
                false;
            _MTime ->
                true
        end
    end),

    case Ignore of
        false ->

            DecoderMethod = decoder_method(DecoderName),
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

handle2(_ProtocolVersion, #requestseqdiff{space_id = SpaceId, dbname = DbName, since_seq = SinceBin} = BaseRequest) ->
    SinceSeqInfo = dbsync_utils:normalize_seq_info(dbsync_utils:decode_term(SinceBin)),
    SeqReq = dbsync_utils:seq_info_to_url(SinceSeqInfo),

    {ok, "200", _, Data} = ibrowse:send_req("http://127.0.0.1:5984/" ++ utils:ensure_list(DbName) ++ "/_changes?feed=normal&include_docs=true&since=" ++ SeqReq, [], get, []),
    {ChangedDocs, ReqSeqInfo} = dbsync_utils:changes_json_to_docs(Data),
    ChangedDocs1 = [ChangedDoc || {#db_document{}, _} = ChangedDoc <- ChangedDocs],

    SpacesMap = lists:foldl(
        fun({#db_document{uuid = UUID, rev_info = {RevNum, [RevHash | _]}} = Doc, CSeq}, Acc) ->
            try
                {ok, #space_info{providers = SyncWith, space_id = SpaceId} = SpaceInfo} = get_space_ctx(DbName, Doc),
                SyncWithSorted = lists:usort(SyncWith),
                {ok, [{ok, #doc{revs = RevInfo}}]} = dao_lib:apply(dao_helper, open_revs, [DbName, UUID, [{RevNum, RevHash}], []], 1),
                NewDoc = Doc#db_document{rev_info = RevInfo},

                {_, LastSSeq, SpaceDocs} = maps:get(SpaceId, Acc, {SpaceInfo, 0, []}),
                maps:put(SpaceId, {SpaceInfo, max(LastSSeq, CSeq), [NewDoc | SpaceDocs]}, Acc)
            catch
                _:{badmatch,{error,{not_found,missing}}} ->
                    Acc;
                _:Reason ->
                    ?error_stacktrace("OMG2 ==============================> ~p", [Reason]),
                    Acc
            end
        end, #{}, ChangedDocs1),

    {SpaceInfo, _, Docs} = maps:get(SpaceId, SpacesMap),
    DocsEncoded = [dbsync_utils:encode_term(Doc) || Doc <- lists:reverse(Docs)],
    #docupdated{dbname = DbName, document =  DocsEncoded, prev_seq = SinceBin, curr_seq = dbsync_utils:encode_term(ReqSeqInfo)};

handle2(_ProtocolVersion, _Msg) ->
    ?warning("dbsync: unknown request: ~p", [_Msg]),
    unknown_request.





get_current_seq(ProviderId, SpaceId, DbName) ->
    case state_get(last_space_seq_key(ProviderId, SpaceId, DbName)) of
        undefined ->
            {0, <<>>};
        CurrSeqInfo -> CurrSeqInfo
    end.


handle_broadcast(ProtocolVersion, SpaceId, #docupdated{dbname = DbName, document = DocsData, prev_seq = PrevSeqInfoBin, curr_seq = CurrSeqInfoBin} = Request, BaseRequest) ->
    {provider_id, ProviderId} = get(peer_id),
    CurrSeqInfo = dbsync_utils:decode_term(CurrSeqInfoBin),
    PrevSeqInfo = dbsync_utils:decode_term(PrevSeqInfoBin),
    lists:foreach(
        fun(DocData) ->
            #db_document{uuid = DocUUID, rev_info = {_, [EmitedRev | _]}} = Doc = dbsync_utils:decode_term(DocData),
            DocDbName = doc_to_db(Doc),
            {ok, #space_info{space_id = SpaceId} = SpaceInfo} = get_space_ctx(DocDbName, Doc),
            case dao_lib:apply(dao_records, save_record, [DocDbName, Doc, [replicated_changes]], ProtocolVersion) of
                {ok, _} ->
                    ?info("UPDATED ~p OMG !!!!", [DocUUID]),
                    ok;
                {error, Reason2} ->
                    ?error("=================> Error2 while replicating changes ~p", [Reason2]),
                    {error, Reason2}

            end
        end, DocsData),
    sync_call(fun(_State) ->
        case get_current_seq(ProviderId, SpaceId, DbName) of
            PrevSeqInfo ->
                ?debug("State of space ~p updated to ~p!", [SpaceId, CurrSeqInfo]),
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



send_direct_message(ProviderId, Request, {AnswerDecoderName, AnswerType} = AnswerConf, Attempts) when Attempts > 0 ->
    PushTo = ProviderId,
    ReqEncoder = encoder_method(get_message_type(Request)),
    RequestData = iolist_to_binary(apply(dbsync_pb, ReqEncoder, [Request])),
    MsgId = provider_proxy_con:get_msg_id(),

    RTRequest = #rtrequest{answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = RequestData, message_decoder_name = a2l(dbsync),
        message_id = MsgId, message_type = a2l(utils:record_type(Request)), module_name = a2l(dbsync), protocol_version = 1, synch = true},
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
                    erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]);
                InvalidStatus ->
                    ?error("Cannot reroute message ~p due to invalid answer status: ~p", [get_message_type(Request), InvalidStatus]),
                    send_direct_message(ProviderId, Request, AnswerConf, Attempts - 1)
            end
    after Timeout ->
        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
        send_direct_message(ProviderId, Request, AnswerConf, Attempts - 1)
    end.

request_diff(ProviderId, SpaceId, DbName, SinceSeqInfo, Attempts) ->
    Request = #requestseqdiff{space_id = SpaceId, dbname = utils:ensure_binary(DbName), since_seq = dbsync_utils:encode_term(SinceSeqInfo)},
    #docupdated{document = DocsData, curr_seq = CurrSeqInfoBin, prev_seq = PrevSeqInfoBin} = send_direct_message(ProviderId, Request, {dbsync, docupdated}, Attempts),
    CurrSeqInfo = dbsync_utils:decode_term(CurrSeqInfoBin),
    PrevSeqInfo = dbsync_utils:decode_term(PrevSeqInfoBin),
    lists:foreach(
        fun(DocData) ->
            #db_document{uuid = DocUUID, rev_info = {_, [_EmitedRev | _]}} = Doc = dbsync_utils:decode_term(DocData),
            DocDbName = doc_to_db(Doc),
            try get_space_ctx(DocDbName, Doc) of
                {ok, #space_info{space_id = SpaceId} = _SpaceInfo} ->
                    ok;
                {error, _} -> ok
            catch
                _:_ -> ok
            end,
            case dao_lib:apply(dao_records, save_record, [DocDbName, Doc, [replicated_changes]], 1) of
                {ok, _} ->
                    ?debug("UPDATED ~p OMG !!!!", [DocUUID]),
                    ok;
                {error, Reason2} ->
                    ?error("Cannot replicate changes dur to: ~p", [Reason2]),
                    {error, Reason2}

            end
        end, DocsData),

    sync_call(fun(_State) ->
        case get_current_seq(ProviderId, SpaceId, DbName) of
            PrevSeqInfo ->
                ?debug("State of space ~p updated to ~p!", [SpaceId, CurrSeqInfo]),
                ets:insert(?dbsync_state, {last_space_seq_key(ProviderId, SpaceId, DbName), CurrSeqInfo});
            UnknownSeq ->
                ?debug("Cannot update database with requested diff due to seq missmatch ~p vs local ~p", [PrevSeqInfo, UnknownSeq])
        end
    end),
    ok.

emit_documents(DbName, DocsWithSeq, SinceSeqInfo) ->
    SpacesMap = lists:foldl(
        fun({#db_document{uuid = UUID, rev_info = {RevNum, [RevHash | _]}} = Doc, CSeq}, Acc) ->
            try
                {ok, #space_info{providers = SyncWith, space_id = SpaceId} = SpaceInfo} = get_space_ctx(DbName, Doc),
                SyncWithSorted = lists:usort(SyncWith),
                {ok, [{ok, #doc{revs = RevInfo}}]} = dao_lib:apply(dao_helper, open_revs, [DbName, UUID, [{RevNum, RevHash}], []], 1),
                NewDoc = Doc#db_document{rev_info = RevInfo},

                {_, LastSSeq, SpaceDocs} = maps:get(SpaceId, Acc, {SpaceInfo, 0, []}),
                maps:put(SpaceId, {SpaceInfo, max(LastSSeq, CSeq), [NewDoc | SpaceDocs]}, Acc)
            catch
                _:{badmatch,{error,{not_found,missing}}} ->
                    Acc;
                _:Reason ->
                    ?error_stacktrace("OMG2 ==============================> ~p", [Reason]),
                    Acc
            end
        end, #{}, DocsWithSeq),

    lists:foreach(
        fun({SpaceId, {#space_info{providers = SyncWith} = SpaceInfo, LastSpaceSeq, Docs}}) ->
            Docs1 = lists:reverse(Docs),
            sync_call(fun(_) ->
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


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
    ok.


get_space_ctx(_DbName, #db_document{uuid = UUID} = Doc) ->
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
    {ok, Doc} = dao_lib:apply(dao_records, get_record, [DbName, UUID, []], 1),
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


push_doc_changes(#space_info{} = _SpaceInfo, _Docs, _, _, []) ->
    ok;
push_doc_changes(#space_info{} = _SpaceInfo, [], _, _, _) ->
    ok;
push_doc_changes(#space_info{} = SpaceInfo, Docs, SinceSeqInfo, LastSpaceSeq, SyncWith) ->
    [SomeDoc | _] = Docs,
    Request = #docupdated{dbname = utils:ensure_binary(doc_to_db(SomeDoc)), document = [dbsync_utils:encode_term(Doc) || Doc <- Docs], curr_seq = dbsync_utils:encode_term(LastSpaceSeq), prev_seq = dbsync_utils:encode_term(SinceSeqInfo)},
    ok = tree_broadcast(SpaceInfo, SyncWith, Request, 3).


tree_broadcast(SpaceInfo, SyncWith, Request, Attempts) ->
    ReqEncoder = encoder_method(get_message_type(Request)),
    BaseRequest = #treebroadcast{request_id = dbsync_utils:gen_request_id(), input = <<"">>, message_type = a2l(ReqEncoder), excluded_providers = [], ledge = <<"">>, redge = <<"">>, depth = 0, space_id = <<"">>},
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
                    ?error("Cannot send message ~p due to invalid answer status: ~p", [get_message_type(SyncRequest), InvalidStatus]),
                    do_emit_tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts - 1)
            end
    after Timeout ->
        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
        do_emit_tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts - 1)
    end;
do_emit_tree_broadcast(_SpaceInfo, _SyncWith, _Request, _BaseRequest, 0) ->
    {error, 'todo_error_code'}.


broadcast_space_state(SpaceId) ->
    {ok, #space_info{providers = SyncWith} = SpaceInfo} = fslogic_objects:get_space({uuid, SpaceId}),
    SeqInfo =
        case ets:lookup(?dbsync_state, last_space_seq_key(SpaceId, ?FILES_DB_NAME)) of
            [{_, SeqInfo1}] ->
                SeqInfo1;
            _ ->
                [{_, SeqInfo2}] = ets:lookup(?dbsync_state, {last_seq, ?FILES_DB_NAME}),
                SeqInfo2
        end,
    Request = #reportspacestate{current_seq = [#reportspacestate_currentseqdata{dbname = utils:ensure_binary(?FILES_DB_NAME), seq_num = dbsync_utils:encode_term(SeqInfo)}]},
    tree_broadcast(SpaceInfo, lists:usort(SyncWith), Request, 3).


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





doc_to_db(#db_document{record = #file{}}) ->
    ?FILES_DB_NAME;
doc_to_db(#db_document{record = #file_meta{}}) ->
    ?FILES_DB_NAME.


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
                       ?error("Unknown ============================> ~p", [Unk]),
                       ?dbsync_cast({changes_stream, StreamId, eof}),
                       State
               after timer:seconds(10) ->
                   ?dbsync_cast({changes_stream, StreamId, eof}),
                   State
               end,
    changes_receiver_loop({StreamId, NewState}).


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
                        ok = tree_broadcast(SpaceInfo, lists:usort(SyncWith), Request, 3)
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
        case gr_providers:get_spaces(provider) of
            {ok, SpaceIds} ->
                SpaceIds;
            {error, Reason} ->
                Spaces
        end,
    Res = [catch broadcast_space_state(SpaceId) || SpaceId <- NewSpaces],
    ?debug("DBSync ping result: ~p", [Res]),

    catch state_save(),

    timer:sleep(timer:seconds(30)),
    ping_service_loop(NewSpaces).


last_space_seq_key(SpaceId, DbName) ->
    last_space_seq_key(cluster_manager_lib:get_provider_id(), SpaceId, DbName).

last_space_seq_key(ProviderId, SpaceId, DbName) ->
    {last_space_seq, ProviderId, utils:ensure_binary(SpaceId), utils:ensure_binary(DbName)}.


state_save() ->
    State = ets:tab2list(?dbsync_state),
    LocalProviderId = cluster_manager_lib:get_provider_id(),
    Ignore1 = [Entry || {{uuid_to_spaceid, _}, _} = Entry <- State],

    Ignore2 = [Entry || {{last_space_seq, LocalProviderId1, _, _}, _} = Entry <- State, LocalProviderId1 =:= LocalProviderId],
    State1 = State -- Ignore1,
    State2 = State1 -- Ignore2,

    case dao_lib:apply(dao_records, save_record, [?SYSTEM_DB_NAME, #db_document{record = #dbsync_state{ets_list = State2}, force_update = true, uuid = "dbsync_state"}, []], 1) of
        {ok, _} -> ok;
        {error, Reason} ->
            ?error("Cannot save DBSync state due to ~p", [Reason]),
            {error, Reason}
    end.


state_load() ->
    case dao_lib:apply(dao_records, get_record, [?SYSTEM_DB_NAME, "dbsync_state", []], 1) of
        {ok, #db_document{record = #dbsync_state{ets_list = ETSList}}} ->
            [ets:insert(?dbsync_state, Elem) || Elem <- ETSList],
            ok;
        {error, Reason} ->
            ?warning("Cannot load DBSync's state due to: ~p", Reason),
            {error, Reason}
    end.


changes_receiver_name(StreamId) ->
    list_to_atom("changes_receiver_" ++ utils:ensure_list(StreamId)).


state_set(Key, Value) ->
    ets:insert(?dbsync_state, {Key, Value}).

state_get(Key) ->
    case ets:lookup(?dbsync_state, Key) of
        [{_, Value}] -> Value;
        _ -> undefined
    end.


state_loop() ->
    state_loop(#dbsync_state{}).
state_loop(State) ->
    NewState =
        receive
            {From, Fun} when is_function(Fun) ->
                {Response, NState} =
                    try Fun(State) of
                        {Response1, #dbsync_state{} = NState1} ->
                            {Response1, NState1};
                        OnlyResp -> {OnlyResp, State}
                    catch
                        Type:Error -> {{error, {Type, Error}}, State}
                    end,
                From ! {self(), Response},
                NState;
            {From, Module, Method, Args} ->
                {Response, NState} =
                    try apply(Module, Method, Args ++ [State]) of
                        {Response1, #dbsync_state{} = NState1} ->
                            {Response1, NState1};
                        OnlyResp -> {OnlyResp, State}
                    catch
                        Type:Error ->
                            Stack = erlang:get_stacktrace(),
                            ?dump_all([Type, Error, Stack]),
                            {{error, {Type, Error, Stack}}, State}
                    end,
                From ! {self(), Response},
                NState
        after 10000 ->
            State
        end,
    ?MODULE:state_loop(NewState).

sync_call(Fun) when is_function(Fun) ->
    ?dbsync_state ! {self(), Fun},
    sync_call_get_response();
sync_call(Method) when is_atom(Method) ->
    sync_call(Method, []).
sync_call(Method, Args) ->
    sync_call(rtransfer_sync, Method, Args).
sync_call(Module, Method, Args) when is_atom(Module), is_atom(Method), is_list(Args) ->
    ?dbsync_state ! {self(), Module, Method, Args},
    sync_call_get_response().

%% Interal sync_call use only !
sync_call_get_response() ->
    StPid = whereis(?dbsync_state),
    receive
        {StPid, Response} -> Response
    after 10000 ->
        {error, sync_timeout}
    end.