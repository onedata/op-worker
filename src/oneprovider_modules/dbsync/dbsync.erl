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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-define(dbsync_state, dbsync_state).

%% API
-export([init/1, handle/2, cleanup/0]).


%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
    ?info("dbsync initialized"),
    ets:new(?dbsync_state, [public, named_table, set]),
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

handle(_ProtocolVersion, {{event, doc_saved}, {?FILES_DB_NAME, #db_document{rev_info = {_OldSeq, OldRevs}} = Doc, {NewSeq, NewRev}, Opts}}) ->
    {ok, #space_info{providers = SyncWith} = SpaceInfo} = get_space_ctx(Doc),
    ?info("SpaceInfo for doc ~p: ~p", [Doc, SpaceInfo]),

    NewDoc = Doc#db_document{rev_info = {NewSeq, [NewRev | OldRevs]}},
    SyncWithSorted = lists:usort(SyncWith),

    ok = push_doc_changes(SpaceInfo, NewDoc, SyncWithSorted),
    ok = push_doc_changes(SpaceInfo, NewDoc, SyncWithSorted);

handle(_ProtocolVersion, {{event, doc_saved}, {DbName, Doc, NewRew, Opts}}) ->
    ?info("OMG wrong DB ~p", [DbName]),
    ok;


handle(_ProtocolVersion, Request) ->
    ?info("Hello ~p", [Request]);

handle(_ProtocolVersion, _Msg) ->
    ?warning("dbsync: wrong request: ~p", [_Msg]),
    wrong_request.


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
    ok.


get_space_ctx(#db_document{uuid = UUID} = Doc) ->
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
    end.
get_space_ctx2(#db_document{uuid = "", record = #file{}}, []) ->
    {error, no_space};
get_space_ctx2(#db_document{uuid = UUID, record = #file{extensions = Exts, parent = Parent}} = Doc, UUIDs) ->
    case lists:keyfind(?file_space_info_extestion, 1, Exts) of
        {?file_space_info_extestion, #space_info{} = SpaceInfo} ->
            {ok, {UUIDs, SpaceInfo}};
        false ->
            {ok, ParentDoc} = dao_lib:apply(vfs, get_file, [{uuid, Parent}], 1),
            get_space_ctx2(ParentDoc, [UUID | UUIDs])
    end.

push_doc_changes(#space_info{} = SpaceInfo, Doc, SyncWith) ->
    SyncWith1 = SyncWith -- [cluster_manager_lib:get_provider_id()],
    {LSync, RSync} = lists:split(crypto:rand_uniform(0, length(SyncWith1)), SyncWith1),
    ExclProviders = [cluster_manager_lib:get_provider_id()],
    do_emit_doc_changes(SpaceInfo, Doc, LSync, #docupdated{excluded_providers = ExclProviders}, 3),
    do_emit_doc_changes(SpaceInfo, Doc, RSync, #docupdated{excluded_providers = ExclProviders}, 3).

do_emit_doc_changes(SpaceInfo, Doc, [], #docupdated{}, _) ->
    ok;
do_emit_doc_changes(SpaceInfo, Doc, SyncWith, #docupdated{depth = Depth} = Request, Attempts) when Attempts > 0 ->
    PushTo = lists:nth(crypto:rand_uniform(1, 1 + length(SyncWith))),
    [LEdge | _] = SyncWith,
    REdge = lists:last(SyncWith),
    DocBinary = term_to_binary(Doc),
    SyncRequest = Request#docupdated{ledge = LEdge, redge = REdge, document = DocBinary, depth = Depth + 1},
    SyncRequestData = dbsync_pb:encode_docupdated(SyncRequest),
    MsgId = provider_proxy_con:get_msg_id(),


    {AnswerDecoderName, AnswerType} = {pb_module(rt_core), atom},


    RTRequest = #rtrequest{answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = SyncRequestData, message_decoder_name = "dbsync_pb",
                            message_id = MsgId, message_type = a2l(utils:record_type(SyncRequest)), module_name = "dbsync", protocol_version = 1, synch = true},
    RTRequestData = rtcore_pb:encode_rtrequest(RTRequest),

    URL = get_provider_url(PushTo),
    Timeout = 1000,
    provider_proxy_con:send({URL, <<"oneprovider">>}, MsgId, RTRequestData),
    receive
        {response, MsgId, AnswerStatus, WorkerAnswer} ->
            provider_proxy_con:report_ack({URL, <<"oneprovider">>}),
            ?debug("Answer for inter-provider pull request: ~p ~p", [AnswerStatus, WorkerAnswer]),
            case AnswerStatus of
                ?VOK ->
                    Answer = erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]),
                    Answer;
                InvalidStatus ->
                    ?error("Cannot reroute message ~p due to invalid answer status: ~p", [get_message_type(SyncRequest), InvalidStatus]),
                    do_emit_doc_changes(SpaceInfo, Doc, SyncWith, Request, Attempts - 1)
            end
    after Timeout ->
        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
        do_emit_doc_changes(SpaceInfo, Doc, SyncWith, Request, Attempts - 1)
    end.


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