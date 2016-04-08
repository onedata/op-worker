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
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([const_get/1, get_session_supervisor_and_node/1, get_event_manager/1,
    get_event_managers/0, get_sequencer_manager/1, get_random_connection/1,
    get_connections/1, get_auth/1, remove_connection/2, get_rest_session_id/1,
    all_with_user/0]).

-type id() :: binary().
-type auth() :: #auth{}.
-type type() :: fuse | rest | gui | provider_outgoing | provider.
-type status() :: active | inactive | phantom.
-type identity() :: #identity{}.

-export_type([id/0, auth/0, type/0, status/0, identity/0]).

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
    Timestamp = os:timestamp(),
    datastore:save(?STORE_LEVEL, Document#document{value = Sess#session{
        accessed = Timestamp
    }}).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) when is_map(Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff#{accessed => os:timestamp()});
update(Key, Diff) when is_function(Diff) ->
    NewDiff = fun(Sess) ->
        case Diff(Sess) of
            {ok, NewSess} -> {ok, NewSess#session{accessed = os:timestamp()}};
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
    Timestamp = os:timestamp(),
    datastore:create(?STORE_LEVEL, Document#document{value = Sess#session{
        accessed = Timestamp
    }}).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Sets access time to current time for user session and returns old value.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(?ROOT_SESS_ID) ->
    {ok, #document{key = ?ROOT_SESS_ID, value = #session{
        identity = #identity{user_id = ?ROOT_USER_ID}
    }}};
get(Key) ->
    case datastore:get(?STORE_LEVEL, ?MODULE, Key) of
        {ok, Doc} ->
            session:update(Key, #{}),
            {ok, Doc};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Does not modify access time.
%% @end
%%--------------------------------------------------------------------
-spec const_get(datastore:key()) ->
    {ok, datastore:document()} | datastore:get_error().
const_get(Key) ->
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
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    case ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)) of
        true ->
            update(Key, #{}),
            true;
        false ->
            false
    end.

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
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
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
            value = #session{identity = #identity{user_id = UserID}}
        } = Doc, Acc) when is_binary(UserID) ->
            {next, [Doc | Acc]};
        (_X, Acc) ->
            {next, Acc}
    end,
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, Filter, []).
%%--------------------------------------------------------------------
%% @doc
%% Returns session supervisor and node on which supervisor is running.
%% @end
%%--------------------------------------------------------------------
-spec get_session_supervisor_and_node(SessId :: id()) ->
    {ok, {SessSup :: pid(), Node :: node()}} | {error, Reason :: term()}.
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
-spec get_event_manager(SessId :: id()) ->
    {ok, EvtMan :: pid()} | {error, Reason :: term()}.
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
%% Returns list of all event managers associated with any session.
%% @end
%%--------------------------------------------------------------------
-spec get_event_managers() -> {ok, [{ok, EvtMan :: pid()} | {error, {not_found,
    SessId :: session:id()}}]} | {error, Reason :: term()}.
get_event_managers() ->
    case session:list() of
        {ok, Docs} ->
            {ok, lists:map(fun
                (#document{key = SessId, value = #session{event_manager = undefined}}) ->
                    {error, {not_found, SessId}};
                (#document{value = #session{event_manager = EvtMan}}) ->
                    {ok, EvtMan}
            end, Docs)};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns sequencer manager associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_manager(SessId :: id()) ->
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
-spec get_random_connection(SessId :: session:id()) ->
    {ok, Con :: pid()} | {error, Reason :: empty_connection_pool | term()}.
get_random_connection(SessId) ->
    case get_connections(SessId) of
        {ok, []} -> {error, empty_connection_pool};
        {ok, Cons} -> {ok, utils:random_element(Cons)};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(SessId :: id()) ->
    {ok, Comm :: pid()} | {error, Reason :: term()}.
get_connections(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(ProxyVia)  ->
            ProxyViaSession = session_manager:get_provider_session_id(outgoing, ProxyVia),
            provider_communicator:ensure_connected( ProxyViaSession ),
            get_connections(ProxyViaSession);
        {ok, #document{value = #session{connections = Cons}}} ->
            {ok, Cons};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes connection from session and if it was the last connection schedules
%% session removal.
%% @end
%%--------------------------------------------------------------------
-spec remove_connection(SessId :: session:id(), Con :: pid()) ->
    ok | datastore:update_error().
remove_connection(SessId, Con) ->
    Diff = fun(#session{watcher = Watcher, connections = Cons} = Sess) ->
        NewCons = lists:filter(fun(C) -> C =/= Con end, Cons),
        case NewCons of
            [] -> gen_server:cast(Watcher, schedule_session_status_checkup);
            _ -> ok
        end,
        {ok, Sess#session{connections = NewCons}}
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} -> ok;
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns #auth{} record associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_auth(SessId :: id()) ->
    {ok, Auth :: #auth{}} | {ok, undefined} | {error, Reason :: term()}.
get_auth(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{auth = Auth}}} ->
            {ok, Auth};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns rest session id for given identity.
%% @end
%%--------------------------------------------------------------------
-spec get_rest_session_id(session:identity()) -> id().
get_rest_session_id(#identity{user_id = Uid}) ->
    <<Uid/binary, "_rest_session">>.

%%%===================================================================
%%% Internal functions
%%%===================================================================

