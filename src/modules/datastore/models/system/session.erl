%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model.
%%% @end
%%%-------------------------------------------------------------------
-module(session).
-author("Tomasz Lichon").
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API - basic model function
-export([
    create/1, save/1, get/1, exists/1, list/0,
    update/2, update_doc_and_time/2, delete/1, delete_doc/1
]).
-export([is_space_owner/2]).

%% API - field access functions
-export([get_session_supervisor_and_node/1]).
-export([get_event_manager/1, get_sequencer_manager/1]).
-export([get_credentials/1, get_data_constraints/1, get_user_id/1]).
-export([get_mode/1]).
-export([set_direct_io/3]).

% exometer callbacks
-export([init_counters/0, init_report/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: binary().
-type record() :: #session{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type ttl() :: non_neg_integer().
-type grace_period() :: non_neg_integer().
-type type() :: fuse | rest | gui | offline | provider_outgoing | provider_incoming | root | guest.
% Supported session modes:
% - normal
% - open_handle - special session mode in which user traverses open handle shares tree
%                 (listing space dir return virtual share root dirs corresponding to
%                 shares having open handle instead of space dirs and files) and is
%                 treated as guest when checking privileges.
-type mode() :: normal | open_handle.
% All sessions, beside root and guest (they start with active status),
% start with initializing status. When the last component of supervision tree
% gets up (either incoming_session_watcher or outgoing_connection_manager),
% meaning entire supervision tree finished getting up, it will set session
% status to active.
-type status() :: initializing | active | inactive.

-export_type([
    id/0, record/0, doc/0,
    ttl/0, grace_period/0,
    type/0, mode/0, status/0
]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true,
    memory_copies => all
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
        accessed = global_clock:timestamp_seconds()
    }})).

%%--------------------------------------------------------------------
%% @doc
%% Saves session.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc = #document{value = Sess}) ->
    ?extract_key(datastore_model:save(?CTX, Doc#document{value = Sess#session{
        accessed = global_clock:timestamp_seconds()
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
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(SessId, Diff) when is_function(Diff) ->
    datastore_model:update(?CTX, SessId, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Updates session and sets new access time.
%% @end
%%--------------------------------------------------------------------
-spec update_doc_and_time(id(), diff()) -> {ok, doc()} | {error, term()}.
update_doc_and_time(SessId, Diff) when is_function(Diff) ->
    Diff2 = fun(Sess) ->
        case Diff(Sess) of
            {ok, NewSess} ->
                {ok, NewSess#session{accessed = global_clock:timestamp_seconds()}};
            {error, Reason} ->
                {error, Reason}
        end
    end,
    datastore_model:update(?CTX, SessId, Diff2).

%%--------------------------------------------------------------------
%% @doc
%% Deletes session document only.
%% @end
%%--------------------------------------------------------------------
-spec delete_doc(id()) -> ok | {error, term()}.
delete_doc(SessId) ->
    datastore_model:delete(?CTX, SessId).

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

-spec is_space_owner(id() | record() | doc(), od_space:id()) -> boolean().
is_space_owner(Session, SpaceId) ->
    {ok, UserId} = get_user_id(Session),
    space_logic:is_owner(SpaceId, UserId).

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
get_user_id(#session{identity = ?SUB(root, ?ROOT_USER_ID)}) ->
    {ok, ?ROOT_USER_ID};
get_user_id(#session{identity = ?SUB(nobody, ?GUEST_USER_ID)}) ->
    {ok, ?GUEST_USER_ID};
get_user_id(#session{identity = ?SUB(user, UserId)}) ->
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
%% Returns credentials associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_credentials
    (id()) -> {ok, undefined | auth_manager:credentials()} | {error, term()};
    (record() | doc()) -> auth_manager:credentials().
get_credentials(?ROOT_SESS_ID) ->
    {ok, ?ROOT_CREDENTIALS};
get_credentials(?GUEST_SESS_ID) ->
    {ok, ?GUEST_CREDENTIALS};
get_credentials(<<_/binary>> = SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{credentials = Credentials}}} ->
            {ok, Credentials};
        {error, _} = Error ->
            Error
    end;
get_credentials(#session{credentials = Credentials}) ->
    Credentials;
get_credentials(#document{value = Session}) ->
    get_credentials(Session).


-spec get_data_constraints(record() | doc()) -> data_constraints:constraints().
get_data_constraints(#session{data_constraints = DataConstraints}) ->
    DataConstraints;
get_data_constraints(#document{value = Session}) ->
    get_data_constraints(Session).


-spec get_mode(id() | record() | doc()) -> {ok, mode()} | {error, term()}.
get_mode(<<_/binary>> = SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{mode = SessMode}}} ->
            {ok, SessMode};
        {error, _} = Error ->
            Error
    end;
get_mode(#session{mode = SessMode}) ->
    {ok, SessMode};
get_mode(#document{value = #session{mode = SessMode}}) ->
    {ok, SessMode}.


%%--------------------------------------------------------------------
%% @doc
%% Sets direct_io property of session.
%% @end
%%--------------------------------------------------------------------
-spec set_direct_io(id(), od_space:id(), boolean()) -> ok | {error, term()}.
set_direct_io(SessId, SpaceId, Value) ->
    Diff = fun(Sess = #session{direct_io = DirectIO}) ->
        {ok, Sess#session{direct_io = DirectIO#{SpaceId => Value}}}
    end,
    case session:update(SessId, Diff) of
        {ok, #document{key = SessId}} -> ok;
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
