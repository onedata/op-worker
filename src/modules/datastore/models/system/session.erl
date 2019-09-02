%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model.
% TODO - powinna byc na kazdym node (i dane uzytkownia tez) - przynajnmniej ta czesc sluzaca kazdemu requestowi
%%% @end
%%%-------------------------------------------------------------------
-module(session).
-author("Tomasz Lichon").
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API - basic model function
-export([create/1, save/1, get/1, exists/1, list/0, update/2, delete/1]).
%% API - link functions
-export([add_links/4, get_link/3, fold_links/3, delete_links/3]).
-export([add_local_links/4, get_local_link/3, fold_local_links/3,
    delete_local_links/3]).
%% API - field access functions
-export([get_session_supervisor_and_node/1]).
-export([get_event_manager/1, get_sequencer_manager/1]).
-export([get_auth/1, get_user_id/1]).
-export([set_direct_io/2]).

% exometer callbacks
-export([init_counters/0, init_report/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: binary().
-type record() :: #session{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type ttl() :: non_neg_integer().
-type auth() :: #token_auth{} | ?ROOT_AUTH | ?GUEST_AUTH.
-type type() :: fuse | rest | gui | provider_outgoing | provider_incoming | root | guest.
% All sessions, beside root and guest (they start with active status),
% start with initializing status. When the last component of supervision tree
% gets up (either incoming_session_watcher or outgoing_connection_manager),
% meaning entire supervision tree finished getting up, it will set session
% status to active.
-type status() :: initializing | active | inactive.
-type identity() :: #user_identity{}.

-export_type([id/0, record/0, doc/0, ttl/0, auth/0, type/0, status/0, identity/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true
}).

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).

%%%===================================================================
%%% API - basic model functions
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
%% Returns the list of all sessions.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

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
    ?update_counter(?EXOMETER_NAME(active_sessions), -1),
    session_helpers:delete_helpers(SessId),
    session_handles:remove_handles(SessId),
    session_open_files:invalidate_entries(SessId),
    datastore_model:delete(?CTX, SessId).

%%%===================================================================
%%% API - link functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds link to a tree.
%% @end
%%--------------------------------------------------------------------
-spec add_links(id(), datastore:tree_id(), datastore:link_name(),
    datastore:link_target()) -> ok | {error, term()}.
add_links(SessId, TreeID, HandleId, Key) ->
    ?extract_ok(datastore_model:add_links(?CTX, SessId,
        TreeID, {HandleId, Key}
    )).

%%--------------------------------------------------------------------
%% @doc
%% Gets link from a tree.
%% @end
%%--------------------------------------------------------------------
-spec get_link(id(), datastore:tree_id(), datastore:link_name()) ->
    {ok, [datastore:link()]} | {error, term()}.
get_link(SessId, TreeID, HandleId) ->
    datastore_model:get_links(?CTX, SessId, TreeID, HandleId).

%%--------------------------------------------------------------------
%% @doc
%% Iterates over tree and executes Fun on each link.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(id(), datastore:tree_id(),
    datastore:fold_fun(datastore:link())) ->
    {ok, datastore:fold_acc()} | {error, term()}.
fold_links(SessId, TreeID, Fun) ->
    datastore_model:fold_links(?CTX, SessId, TreeID, Fun, [], #{}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from a tree.
%% @end
%%--------------------------------------------------------------------
-spec delete_links
    (id(), datastore:tree_id(), datastore:link_name()) -> ok | {error, term()};
    (id(), datastore:tree_id(), [datastore:link_name()]) -> [ok | {error, term()}].
delete_links(SessId, TreeID, HandleId) ->
    datastore_model:delete_links(?CTX, SessId, TreeID, HandleId).

%%--------------------------------------------------------------------
%% @doc
%% Adds local link to a tree.
%% @end
%%--------------------------------------------------------------------
-spec add_local_links(id(), datastore:tree_id(), datastore:link_name(),
    datastore:link_target()) -> ok | {error, term()}.
add_local_links(SessId, TreeID, HandleId, Key) ->
    ?extract_ok(datastore_model:add_links(?CTX#{routing => local}, SessId,
        TreeID, {HandleId, Key}
    )).

%%--------------------------------------------------------------------
%% @doc
%% Gets local link from a tree.
%% @end
%%--------------------------------------------------------------------
-spec get_local_link(id(), datastore:tree_id(), datastore:link_name()) ->
    {ok, [datastore:link()]} | {error, term()}.
get_local_link(SessId, TreeID, HandleId) ->
    datastore_model:get_links(?CTX#{routing => local}, SessId, TreeID, HandleId).

%%--------------------------------------------------------------------
%% @doc
%% Iterates over local tree and executes Fun on each link.
%% @end
%%--------------------------------------------------------------------
-spec fold_local_links(id(), datastore:tree_id(),
    datastore:fold_fun(datastore:link())) ->
    {ok, datastore:fold_acc()} | {error, term()}.
fold_local_links(SessId, TreeID, Fun) ->
    datastore_model:fold_links(?CTX#{routing => local},
        SessId, TreeID, Fun, [], #{}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes local link from a tree.
%% @end
%%--------------------------------------------------------------------
-spec delete_local_links
    (id(), datastore:tree_id(), datastore:link_name()) -> ok | {error, term()};
    (id(), datastore:tree_id(), [datastore:link_name()]) -> [ok | {error, term()}].
delete_local_links(SessId, TreeID, HandleId) ->
    datastore_model:delete_links(?CTX#{routing => local},
        SessId, TreeID, HandleId).

%%%===================================================================
%%% API - field access functions
%%%===================================================================

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
%% Sets direct_io property of session.
%% @end
%%--------------------------------------------------------------------
-spec set_direct_io(id(), boolean()) -> ok | {error, term()}.
set_direct_io(SessId, DirectIO) ->
    Diff = fun(Sess) ->
        {ok, Sess#session{direct_io = DirectIO}}
    end,
    case session:update(SessId, Diff) of
        {ok, SessId} -> ok;
        Other -> Other
    end.

%%%===================================================================
%%% Exometer callbacks
%%%===================================================================

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
