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
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-define(HELPER_LINK_LEVEL, ?LOCAL_ONLY_LEVEL).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([get_session_supervisor_and_node/1, get_event_manager/1, get_helper/3,
    get_sequencer_manager/1, get_random_connection/1, get_random_connection/2,
    get_connections/1, get_connections/2, get_auth/1, remove_connection/2, get_rest_session_id/1,
    all_with_user/0, get_user_id/1, add_open_file/2, remove_open_file/2,
    get_transfers/1, remove_transfer/2, add_transfer/2, add_handle/3, remove_handle/2, get_handle/2,
    is_special/1, is_root/1, is_guest/1]).

-type id() :: binary().
-type model() :: #session{}.
-type doc() :: #document{value :: model()}.
-type ttl() :: non_neg_integer().
-type auth() :: #token_auth{} | #basic_auth{}.
-type type() :: fuse | rest | gui | provider_outgoing | provider_incoming | root | guest.
-type status() :: active | inactive.
-type identity() :: #user_identity{}.

-export_type([id/0, model/0, doc/0, ttl/0, auth/0, type/0, status/0, identity/0]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(#document{value = Sess} = Document) ->
    datastore:save(?STORE_LEVEL, Document#document{value = Sess#session{
        accessed = erlang:system_time(seconds)
    }}).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) when is_map(Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff#{
        accessed => erlang:system_time(seconds)
    });
update(Key, Diff) when is_function(Diff) ->
    NewDiff = fun(Sess) ->
        case Diff(Sess) of
            {ok, NewSess} -> {ok, NewSess#session{
                accessed = erlang:system_time(seconds)
            }};
            {error, Reason} -> {error, Reason}
        end
    end,
    datastore:update(?STORE_LEVEL, ?MODULE, Key, NewDiff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(#document{value = Sess} = Document) ->
    datastore:create(?STORE_LEVEL, Document#document{value = Sess#session{
        accessed = erlang:system_time(seconds)
    }}).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Sets access time to current time for user session and returns old value.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    case session:get(Key) of
        {ok, #document{key = SessId, value = #session{open_files = OpenFiles}}} ->
            worker_proxy:multicast(?SESSION_MANAGER_WORKER,
                {apply, fun() -> delete_helpers_on_this_node(SessId) end}),

            lists:foreach(fun(FileUUID) ->
                file_handles:invalidate_session_entry(FileUUID, Key)
            end, sets:to_list(OpenFiles));
        _ -> ok
    end,

    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(session_bucket, [], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term(), ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Returns session supervisor and node on which supervisor is running.
%% @end
%%--------------------------------------------------------------------
-spec all_with_user() ->
    {ok, [datastore:document()]} | {error, Reason :: term()}.
all_with_user() ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{
            value = #session{identity = #user_identity{user_id = UserID}}
        } = Doc, Acc) when is_binary(UserID) ->
            {next, [Doc | Acc]};
        (_X, Acc) ->
            {next, Acc}
    end,
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, Filter, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns ID of user associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id
    (id() | model() | doc()) -> {ok, od_user:id()} | {error, Reason :: term()}.
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
    {ok, {SessSup :: pid(), node()}} | {error, Reason :: term()}.
get_session_supervisor_and_node(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{supervisor = undefined}}} ->
            {error, {not_found, missing}};
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
-spec get_event_manager(id()) -> {ok, EvtMan :: pid()} | {error, Reason :: term()}.
get_event_manager(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{event_manager = undefined}}} ->
            {error, {not_found, missing}};
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
    {ok, SeqMan :: pid()} | {error, Reason :: term()}.
get_sequencer_manager(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{sequencer_manager = undefined}}} ->
            {error, {not_found, missing}};
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
    {ok, [Comm :: pid()]} | {error, Reason :: term()}.
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
    {ok, [Comm :: pid()]} | {error, Reason :: term()}.
get_connections(SessId, HideOverloaded) ->
    case ?MODULE:get(SessId) of
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
    ok | datastore:update_error().
remove_connection(SessId, Con) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        NewCons = lists:filter(fun(C) -> C =/= Con end, Cons),
        {ok, Sess#session{connections = NewCons}}
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} -> ok;
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns #token_auth{} record associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_auth
    (id()) -> {ok, Auth :: auth()} | {ok, undefined} | {error, Reason :: term()};
    (model() | doc()) -> auth().
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
    <<(oneprovider:get_provider_id())/binary, "_", Uid/binary, "_rest_session">>.

%%--------------------------------------------------------------------
%% @doc
%% Adds open file UUID to session.
%% @end
%%--------------------------------------------------------------------
-spec add_open_file(id(), file_meta:uuid()) ->
    ok | {error, Reason :: term()}.
add_open_file(SessId, FileUUID) ->
    Diff = fun(#session{open_files = OpenFiles} = Sess) ->
        {ok, Sess#session{open_files = sets:add_element(FileUUID, OpenFiles)}}
    end,

    case update(SessId, Diff) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes open file UUID from session.
%% @end
%%--------------------------------------------------------------------
-spec remove_open_file(id(), file_meta:uuid()) ->
    ok | {error, Reason :: term()}.
remove_open_file(SessId, FileUUID) ->
    Diff = fun(#session{open_files = OpenFiles} = Sess) ->
        {ok, Sess#session{open_files = sets:del_element(FileUUID, OpenFiles)}}
    end,

    case update(SessId, Diff) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes open file UUID from session.
%% @end
%%--------------------------------------------------------------------
-spec get_transfers(id()) -> {ok, [transfer:id()]} | {error, Reason :: term()}.
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
remove_transfer(SessionId, TransferId) ->
    session:update(SessionId, fun(Sess = #session{transfers = Transfers}) ->
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
add_transfer(SessionId, TransferId) ->
    session:update(SessionId, fun(Sess = #session{transfers = Transfers}) ->
        {ok, Sess#session{transfers = [TransferId | Transfers]}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Add link to handle.
%% @end
%%--------------------------------------------------------------------
-spec add_handle(SessionId :: id(), HandleID :: storage_file_manager:handle_id(),
    Handle :: storage_file_manager:handle()) -> ok | datastore:generic_error().
add_handle(SessionId, HandleID, Handle) ->
    case sfm_handle:create(#document{value = Handle}) of
        {ok, Key} ->
            datastore:add_links(?LINK_STORE_LEVEL, SessionId, ?MODEL_NAME, [{HandleID, {Key, sfm_handle}}]);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Remove link to handle.
%% @end
%%--------------------------------------------------------------------
-spec remove_handle(SessionId :: id(), HandleID :: storage_file_manager:handle_id()) ->
    ok | datastore:generic_error().
remove_handle(SessionId, HandleID) ->
    case datastore:fetch_link(?LINK_STORE_LEVEL, SessionId, ?MODEL_NAME, HandleID) of
        {ok, {HandleKey, sfm_handle}} ->
            case sfm_handle:delete(HandleKey) of
                ok ->
                    datastore:delete_links(?LINK_STORE_LEVEL, SessionId, ?MODEL_NAME, [HandleID]);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, link_not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets handle.
%% @end
%%--------------------------------------------------------------------
-spec get_handle(SessionId :: id(), HandleID :: storage_file_manager:handle_id()) ->
    {ok, storage_file_manager:handle()} | datastore:generic_error().
get_handle(SessionId, HandleID) ->
    case datastore:fetch_link_target(?LINK_STORE_LEVEL, SessionId, ?MODEL_NAME, HandleID) of
        {ok, #document{value = Handle}} ->
            {ok, Handle};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves a helper associated with the session by
%% {SessionId, SpaceUuid} key. The helper is created and associated
%% with the session if it doesn't exist.
%% @end
%%--------------------------------------------------------------------
-spec get_helper(id(), SpaceUuid :: file_meta:uuid(), storage:doc()) ->
    {ok, helpers:helper_handle()} | datastore:generic_error().
get_helper(SessionId, SpaceUuid, StorageDoc) ->
    fetch_lock_fetch_helper(SessionId, SpaceUuid, StorageDoc, false).

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
-spec fetch_lock_fetch_helper(id(), SpaceUuid :: file_meta:uuid(), storage:doc(),
    InCriticalSection :: boolean()) ->
    {ok, helpers:helper_handle()} | datastore:generic_error().
fetch_lock_fetch_helper(SessionId, SpaceUuid, StorageDoc, InCriticalSection) ->
    StorageId = storage:get_id(StorageDoc),
    FetchResult = datastore:fetch_link_target(?HELPER_LINK_LEVEL, SessionId,
        ?MODEL_NAME, link_key(SpaceUuid, StorageId)),

    case {FetchResult, InCriticalSection} of
        {{ok, #document{value = Handle}}, _} ->
            {ok, Handle};

        {{error, link_not_found}, false} ->
            critical_section:run({SessionId, SpaceUuid, StorageId}, fun() ->
                fetch_lock_fetch_helper(SessionId, SpaceUuid, StorageDoc, true)
            end);

        {{error, link_not_found}, true} ->
            add_missing_helper(SessionId, SpaceUuid, StorageDoc);

        {{error, Reason}, _} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new #helper_handle{} document in the database and links
%% it with current session.
%% @end
%%--------------------------------------------------------------------
-spec add_missing_helper(id(), SpaceUuid :: file_meta:uuid(),
    storage:doc()) -> {ok, helpers:helper_handle()} | datastore:generic_error().
add_missing_helper(SessionId, SpaceUuid, StorageDoc) ->
    StorageId = storage:get_id(StorageDoc),
    {ok, UserId} = get_user_id(SessionId),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUuid),

    {ok, #document{key = Key, value = HelperHandle}} =
        helper_handle:create(UserId, SpaceId, StorageDoc),

    case datastore:add_links(?HELPER_LINK_LEVEL, SessionId, ?MODEL_NAME,
        [{link_key(StorageId, SpaceUuid),
            {Key, helper_handle}}]) of
        ok ->
            {ok, HelperHandle};

        {error, Reason} ->
            helper_handle:delete(Key),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes all associated helper handles present on the node.
%% @end
%%--------------------------------------------------------------------
-spec delete_helpers_on_this_node(SessId :: id()) ->
    ok | datastore:generic_error().
delete_helpers_on_this_node(SessId) ->
    datastore:foreach_link(?HELPER_LINK_LEVEL, SessId, ?MODEL_NAME,
        fun(_LinkName, {_V, [{_, _, HelperKey, helper_handle}]}, _) ->
            helper_handle:delete(HelperKey)
        end, undefined),
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
