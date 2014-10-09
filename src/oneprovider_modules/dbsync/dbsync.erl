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
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("oneprovider_modules/dao/dao_db_structure.hrl").
-include_lib("ctool/include/logging.hrl").

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

    ok = push_doc_changes(NewDoc, SyncWithSorted),
    ok = push_doc_changes(NewDoc, SyncWithSorted);

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

push_doc_changes(Doc, SyncWith) ->
    SyncWith1 = SyncWith -- [cluster_manager_lib:get_provider_id()],
    {LSync, RSync} = lists:split(crypto:rand_uniform(0, length(SyncWith1)), SyncWith1),
    push_doc_changes2(Doc, LSync, 3),
    push_doc_changes2(Doc, RSync, 3).

push_doc_changes2(Doc, [], _) ->
    ok;
push_doc_changes2(Doc, SyncWith, Attempts) when Attempts > 0 ->
    PushTo = lists:nth(crypto:rand_uniform(1, 1 + length(SyncWith))),
    [LEdge | _] = SyncWith,
    REdge = lists:last(SyncWith).

'+'()
    -> ok.