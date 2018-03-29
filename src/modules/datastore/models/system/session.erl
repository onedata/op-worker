%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model, frequently invoked by incoming tcp
%%% connections in connection
%%% @end
%%%-------------------------------------------------------------------
-module(session).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API
-export([create/1, save/1, get/1, exists/1, update/2, delete/1, list/0]).
-export([get_random_connection/1, get_random_connection/2, remove_connection/2]).
-export([get_connections/1, get_connections/2]).
-export([get_session_supervisor_and_node/1]).
-export([get_event_manager/1, get_sequencer_manager/1]).
-export([get_helper/3]).
-export([get_auth/1, get_user_id/1, get_rest_session_id/1, all_with_user/0]).
-export([add_open_file/2, remove_open_file/2]).
-export([add_transfer/2, get_transfers/1, remove_transfer/2]).
-export([add_handle/3, remove_handle/2, get_handle/2]).
-export([is_special/1, is_root/1, is_guest/1, root_session_id/0]).
-export([set_direct_io/2]).
-export([init_counters/0, init_report/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: binary().
-type record() :: #session{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type ttl() :: non_neg_integer().
-type auth() :: #macaroon_auth{} | ?ROOT_AUTH | ?GUEST_AUTH.
-type type() :: fuse | rest | gui | provider_outgoing | provider_incoming | root | guest.
-type status() :: active | inactive.
-type identity() :: #user_identity{}.

-export_type([id/0, record/0, doc/0, ttl/0, auth/0, type/0, status/0, identity/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true
}).

-define(FILE_HANDLES_TREE_ID, <<"storage_file_handles">>).
-define(HELPER_HANDLES_TREE_ID, <<"helper_handles">>).
-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates session.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc = #document{value = Sess}) ->
    ?update_counter(?EXOMETER_NAME(active_sessions)),
    ?extract_key(datastore_model:create(?CTX, Doc#document{value = Sess#session{
        accessed = time_utils:cluster_time_seconds()
    }})).

%%--------------------------------------------------------------------
%% @doc
%% Saves session.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc = #document{value = Sess}) ->
    ?extract_key(datastore_model:save(?CTX, Doc#document{value = Sess#session{
        accessed = time_utils:cluster_time_seconds()
    }})).

%%--------------------------------------------------------------------
%% @doc
%% Returns session.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(SessId) ->
    datastore_model:get(?CTX, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether session exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Updates session.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(SessId, Diff) when is_function(Diff) ->
    Diff2 = fun(Sess) ->
        case Diff(Sess) of
            {ok, NewSess} ->
                {ok, NewSess#session{
                    accessed = time_utils:cluster_time_seconds()
                }};
            {error, Reason} ->
                {error, Reason}
        end
    end,
    ?extract_key(datastore_model:update(?CTX, SessId, Diff2)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes session.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(SessId) ->
    case session:get(SessId) of
        {ok, #document{key = SessId, value = #session{open_files = OpenFiles}}} ->
            {ok, Nodes} = request_dispatcher:get_worker_nodes(?SESSION_MANAGER_WORKER),
            lists:foreach(fun(Node) ->
                worker_proxy:cast(
                    {?SESSION_MANAGER_WORKER, Node},
                    {apply, fun() -> delete_helpers_on_this_node(SessId) end},
                    undefined,
                    undefined,
                    direct
                )
            end, Nodes),

            lists:foreach(fun(FileGuid) ->
                FileCtx = file_ctx:new_by_guid(FileGuid),
                file_handles:invalidate_session_entry(FileCtx, SessId)
            end, sets:to_list(OpenFiles));
        _ ->
            ok
    end,

    ?update_counter(?EXOMETER_NAME(active_sessions), -1),
    datastore_model:delete(?CTX, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all sessions.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns session supervisor and node on which supervisor is running.
%% @end
%%--------------------------------------------------------------------
-spec all_with_user() ->
    {ok, [datastore:document()]} | {error, Reason :: term()}.
all_with_user() ->
    Fun = fun
        (Doc = #document{value = #session{
            identity = #user_identity{user_id = UserId}
        }}, Acc) when is_binary(UserId) ->
            {ok, [Doc | Acc]};
        (#document{}, Acc) ->
            {ok, Acc}
    end,
    datastore_model:fold(?CTX, Fun, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns Id of user associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id
    (id() | record() | doc()) -> {ok, od_user:id()} | {error, term()}.
get_user_id(<<_/binary>> = SessId) ->
    case session:get(SessId) of
        {ok, Doc} -> get_user_id(Doc);
        {error, Reason} -> {error, Reason}
    end;
get_user_id(#session{identity = #user_identity{user_id = UserId}}) ->
    {ok, UserId};
get_user_id(#document{value = #session{} = Value}) ->
    get_user_id(Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns session supervisor and node on which supervisor is running.
%% @end
%%--------------------------------------------------------------------
-spec get_session_supervisor_and_node(id()) ->
    {ok, {SessSup :: pid(), node()}} | {error, term()}.
get_session_supervisor_and_node(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{supervisor = undefined}}} ->
            {error, not_found};
        {ok, #document{value = #session{supervisor = SessSup, node = Node}}} ->
            {ok, {SessSup, Node}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns event manager associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_event_manager(id()) -> {ok, EvtMan :: pid()} | {error, term()}.
get_event_manager(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{event_manager = undefined}}} ->
            {error, not_found};
        {ok, #document{value = #session{event_manager = EvtMan}}} ->
            {ok, EvtMan};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns sequencer manager associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_manager(id()) ->
    {ok, SeqMan :: pid()} | {error, term()}.
get_sequencer_manager(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{sequencer_manager = undefined}}} ->
            {error, not_found};
        {ok, #document{value = #session{sequencer_manager = SeqMan}}} ->
            {ok, SeqMan};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns random connection associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_random_connection(id()) ->
    {ok, Con :: pid()} | {error, Reason :: empty_connection_pool | term()}.
get_random_connection(SessId) ->
    get_random_connection(SessId, false).

%%--------------------------------------------------------------------
%% @doc
%% Returns random connection associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_random_connection(id(), HideOverloaded :: boolean()) ->
    {ok, Con :: pid()} | {error, Reason :: empty_connection_pool | term()}.
get_random_connection(SessId, HideOverloaded) ->
    case get_connections(SessId, HideOverloaded) of
        {ok, []} -> {error, empty_connection_pool};
        {ok, Cons} -> {ok, utils:random_element(Cons)};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(id()) ->
    {ok, [Comm :: pid()]} | {error, term()}.
get_connections(SessId) ->
    get_connections(SessId, false).

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session. If HideOverloaded is set to true,
%% hides connections that have too long request queue and and removes invalid
%% connections.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(id(), HideOverloaded :: boolean()) ->
    {ok, [Comm :: pid()]} | {error, term()}.
get_connections(SessId, HideOverloaded) ->
    case session:get(SessId) of
        {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(ProxyVia) ->
            ProxyViaSession = session_manager:get_provider_session_id(outgoing, ProxyVia),
            provider_communicator:ensure_connected(ProxyViaSession),
            get_connections(ProxyViaSession, HideOverloaded);
        {ok, #document{value = #session{connections = Cons, watcher = SessionWatcher}}} ->
            case HideOverloaded of
                false ->
                    {ok, Cons};
                true ->
                    NewCons = lists:foldl( %% Foreach connection
                        fun(Pid, AccIn) ->
                            case utils:process_info(Pid, message_queue_len) of
                                undefined ->
                                    %% Connection died, removing from session
                                    ok = session:remove_connection(SessId, Pid),
                                    AccIn;
                                {message_queue_len, QueueLen} when QueueLen > 15 ->
                                    SessionWatcher ! {overloaded_connection, Pid},
                                    AccIn;
                                _ ->
                                    [Pid | AccIn]
                            end
                        end, [], Cons),
                    {ok, NewCons}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes connection from session and if it was the last connection schedules
%% session removal.
%% @end
%%--------------------------------------------------------------------
-spec remove_connection(id(), Con :: pid()) ->
    ok | {error, term()}.
remove_connection(SessId, Con) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        NewCons = lists:filter(fun(C) -> C =/= Con end, Cons),
        {ok, Sess#session{connections = NewCons}}
    end,
    case session:update(SessId, Diff) of
        {ok, _} -> ok;
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns auth record associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_auth
    (id()) -> {ok, Auth :: auth()} | {ok, undefined} | {error, term()};
    (record() | doc()) -> auth().
get_auth(<<_/binary>> = SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{auth = Auth}}} -> {ok, Auth};
        {error, Reason} -> {error, Reason}
    end;
get_auth(#session{auth = Auth}) ->
    Auth;
get_auth(#document{value = Session}) ->
    get_auth(Session).

%%--------------------------------------------------------------------
%% @doc
%% Returns rest session id for given identity.
%% @end
%%--------------------------------------------------------------------
-spec get_rest_session_id(identity()) -> id().
get_rest_session_id(#user_identity{user_id = Uid}) ->
    <<(oneprovider:get_id())/binary, "_", Uid/binary, "_rest_session">>.

%%--------------------------------------------------------------------
%% @doc
%% Adds open file UUId to session.
%% @end
%%--------------------------------------------------------------------
-spec add_open_file(id(), fslogic_worker:file_guid()) ->
    ok | {error, term()}.
add_open_file(SessId, FileGuid) ->
    Diff = fun(#session{open_files = OpenFiles} = Sess) ->
        {ok, Sess#session{open_files = sets:add_element(FileGuid, OpenFiles)}}
    end,

    case update(SessId, Diff) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes open file UUId from session.
%% @end
%%--------------------------------------------------------------------
-spec remove_open_file(id(), fslogic_worker:file_guid()) ->
    ok | {error, term()}.
remove_open_file(SessId, FileGuid) ->
    Diff = fun(#session{open_files = OpenFiles} = Sess) ->
        {ok, Sess#session{open_files = sets:del_element(FileGuid, OpenFiles)}}
    end,

    case update(SessId, Diff) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes open file UUId from session.
%% @end
%%--------------------------------------------------------------------
-spec get_transfers(id()) -> {ok, [transfer:id()]} | {error, term()}.
get_transfers(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{transfers = Transfers}}} ->
            {ok, Transfers};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes transfer from session memory
%% @end
%%--------------------------------------------------------------------
-spec remove_transfer(id(), transfer:id()) -> {ok, datastore:key()}.
remove_transfer(SessId, TransferId) ->
    session:update(SessId, fun(Sess = #session{transfers = Transfers}) ->
        FilteredTransfers = lists:filter(fun(T) ->
            T =/= TransferId
        end, Transfers),
        {ok, Sess#session{transfers = FilteredTransfers}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Add transfer to session memory.
%% @end
%%--------------------------------------------------------------------
-spec add_transfer(id(), transfer:id()) -> {ok, datastore:key()}.
add_transfer(SessId, TransferId) ->
    session:update(SessId, fun(Sess = #session{transfers = Transfers}) ->
        {ok, Sess#session{transfers = [TransferId | Transfers]}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Add link to handle.
%% @end
%%--------------------------------------------------------------------
-spec add_handle(SessId :: id(), HandleId :: storage_file_manager:handle_id(),
    Handle :: storage_file_manager:handle()) -> ok | {error, term()}.
add_handle(SessId, HandleId, Handle) ->
    case sfm_handle:create(#document{value = Handle}) of
        {ok, Key} ->
            ?extract_ok(datastore_model:add_links(?CTX, SessId,
                ?FILE_HANDLES_TREE_ID, {HandleId, Key}
            ));
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Remove link to handle.
%% @end
%%--------------------------------------------------------------------
-spec remove_handle(SessId :: id(), HandleId :: storage_file_manager:handle_id()) ->
    ok | {error, term()}.
remove_handle(SessId, HandleId) ->
    case datastore_model:get_links(
        ?CTX, SessId, ?FILE_HANDLES_TREE_ID, HandleId
    ) of
        {ok, [#link{target = HandleKey}]} ->
            case sfm_handle:delete(HandleKey) of
                ok ->
                    datastore_model:delete_links(
                        ?CTX, SessId, ?FILE_HANDLES_TREE_ID, HandleId
                    );
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets handle.
%% @end
%%--------------------------------------------------------------------
-spec get_handle(SessId :: id(), HandleId :: storage_file_manager:handle_id()) ->
    {ok, storage_file_manager:handle()} | {error, term()}.
get_handle(SessId, HandleId) ->
    case datastore_model:get_links(
        ?CTX, SessId, ?FILE_HANDLES_TREE_ID, HandleId
    ) of
        {ok, [#link{target = HandleKey}]} ->
            case sfm_handle:get(HandleKey) of
                {ok, #document{value = Handle}} ->
                    {ok, Handle};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves a helper associated with the session by
%% {SessId, SpaceUuid} key. The helper is created and associated
%% with the session if it doesn't exist.
%% @end
%%--------------------------------------------------------------------
-spec get_helper(id(), od_space:id(), storage:doc()) ->
    {ok, helpers:helper_handle()} | {error, term()}.
get_helper(SessId, SpaceId, StorageDoc) ->
    fetch_lock_fetch_helper(SessId, SpaceId, StorageDoc, false).

%%--------------------------------------------------------------------
%% @doc
%% Check if session is of special type: root or guest.
%% @end
%%--------------------------------------------------------------------
-spec is_special(id()) -> boolean().
is_special(?ROOT_SESS_ID) ->
    true;
is_special(?GUEST_SESS_ID) ->
    true;
is_special(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if session is of root type.
%% @end
%%--------------------------------------------------------------------
-spec is_root(id()) -> boolean().
is_root(?ROOT_SESS_ID) ->
    true;
is_root(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if session is of guest type.
%% @end
%%--------------------------------------------------------------------
-spec is_guest(id()) -> boolean().
is_guest(?GUEST_SESS_ID) ->
    true;
is_guest(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Returns root session id
%% @end
%%--------------------------------------------------------------------
-spec root_session_id() -> id().
root_session_id() ->
    ?ROOT_SESS_ID.

%%--------------------------------------------------------------------
%% @doc
%% Sets direct_io property of session.
%% @end
%%--------------------------------------------------------------------
-spec set_direct_io(id(), boolean()) ->
    ok | datastore:update_error().
set_direct_io(SessId, DirectIO) ->
    Diff = fun(Sess) ->
        {ok, Sess#session{direct_io = DirectIO}}
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} -> ok;
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes exometer counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    ?init_counters([{?EXOMETER_NAME(active_sessions), counter}]).

%%--------------------------------------------------------------------
%% @doc
%% Sets exometer report connected with counters used by this module.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    ?init_reports([{?EXOMETER_NAME(active_sessions), [value]}]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to fetch a helper handle through link API. If fetching
%% fails with enoent, enters the critical section and retries the
%% request, then inserts a new helper handle if the helper is still missing.
%% The first, out-of-critical-section fetch is an optimization.
%% The fetch+insert occurs in the critical section to avoid
%% instantiating unnecessary helper handles.
%% @end
%%--------------------------------------------------------------------
-spec fetch_lock_fetch_helper(id(), od_space:id(), storage:doc(),
    InCriticalSection :: boolean()) ->
    {ok, helpers:helper_handle()} | {error, term()}.
fetch_lock_fetch_helper(SessId, SpaceId, StorageDoc, InCriticalSection) ->
    Ctx = ?CTX#{routing => local},
    StorageId = storage:get_id(StorageDoc),
    FetchResult = case datastore_model:get_links(Ctx, SessId,
        ?HELPER_HANDLES_TREE_ID, link_key(StorageId, SpaceId)) of
        {ok, [Link = #link{target = Key}]} ->
            {helper_handle:get(Key), Link};
        {error, not_found} ->
            {error, link_not_found};
        {error, Reason} ->
            {error, Reason}
    end,
    case {FetchResult, InCriticalSection} of
        {{{ok, #document{value = Handle}}, _Link}, _} ->
            {ok, Handle};

        {{error, link_not_found}, false} ->
            critical_section:run({SessId, SpaceId, StorageId}, fun() ->
                fetch_lock_fetch_helper(SessId, SpaceId, StorageDoc, true)
            end);

        {{error, link_not_found}, true} ->
            add_missing_helper(SessId, SpaceId, StorageDoc);

        {{{error, not_found}, _Link}, false} ->
            critical_section:run({SessId, SpaceId, StorageId}, fun() ->
                fetch_lock_fetch_helper(SessId, SpaceId, StorageDoc, true)
            end);

        {{{error, not_found}, #link{name = Name}}, true} ->
            %todo this is just temporary fix, VFS-4301
            datastore_model:delete_links(Ctx, SessId, ?HELPER_HANDLES_TREE_ID, Name),
            add_missing_helper(SessId, SpaceId, StorageDoc);

        {{error, Reason2}, _} ->
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new #helper_handle{} document in the database and links
%% it with current session.
%% @end
%%--------------------------------------------------------------------
-spec add_missing_helper(id(), od_space:id(), storage:doc()) ->
    {ok, helpers:helper_handle()} | {error, term()}.
add_missing_helper(SessId, SpaceId, StorageDoc) ->
    Ctx = ?CTX#{routing => local},
    StorageId = storage:get_id(StorageDoc),
    {ok, UserId} = get_user_id(SessId),

    {ok, #document{key = HandleId, value = HelperHandle}} =
        helper_handle:create(SessId, UserId, SpaceId, StorageDoc),

    case datastore_model:add_links(
        Ctx, SessId, ?HELPER_HANDLES_TREE_ID,
        {link_key(StorageId, SpaceId), HandleId}
    ) of
        {ok, _} ->
            {ok, HelperHandle};
        {error, Reason} ->
            helper_handle:delete(HandleId),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes all associated helper handles present on the node.
%% @end
%%--------------------------------------------------------------------
-spec delete_helpers_on_this_node(SessId :: id()) ->
    ok | {error, term()}.
delete_helpers_on_this_node(SessId) ->
    Ctx = ?CTX#{routing => local},
    {ok, Links} = datastore_model:fold_links(Ctx, SessId, ?HELPER_HANDLES_TREE_ID,
        fun(Link = #link{}, Acc) -> {ok, [Link | Acc]} end, [], #{}
    ),
    Names = lists:map(fun(#link{name = Name, target = HandleId}) ->
        helper_handle:delete(HandleId),
        Name
    end, Links),
    datastore_model:delete_links(Ctx, SessId, ?HELPER_HANDLES_TREE_ID, Names),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a key constructed from StorageId and SpaceUuid used for
%% link targets.
%% @end
%%--------------------------------------------------------------------
-spec link_key(StorageId :: storage:id(), SpaceUuid :: file_meta:uuid()) ->
    binary().
link_key(StorageId, SpaceUuid) ->
    <<StorageId/binary, ":", SpaceUuid/binary>>.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
