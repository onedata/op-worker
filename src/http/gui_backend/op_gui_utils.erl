%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module includes utility functions used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_utils).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_user_id/0, get_user_rest_auth/0, get_users_default_space/0]).
-export([register_backend/2, unregister_backend/2]).
-export([get_all_backend_pids/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the user id of currently logged in user, or undefined if
%% this is not a logged in session.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id() -> binary() | undefined.
get_user_id() ->
    case session:get(g_session:get_session_id()) of
        {ok, #document{value = #session{identity = #identity{
            user_id = UserId}}}} ->
            UserId;
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a tuple that can be used directly in REST operations on behalf of
%% current user.
%% @end
%%--------------------------------------------------------------------
-spec get_user_rest_auth() -> {user, {
    Macaroon :: macaroon:macaroon(),
    DischargeMacaroons :: [macaroon:macaroon()]}}.
get_user_rest_auth() ->
    SessionId = g_session:get_session_id(),
    {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),
    #auth{macaroon = Mac, disch_macaroons = DMacs} = Auth,
    {user, {Mac, DMacs}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns the default space of current user (the one in GUI context).
%% @end
%%--------------------------------------------------------------------
-spec get_users_default_space() -> binary().
get_users_default_space() ->
    % @TODO this should be taken from onedata_user record and changes of this
    % should be pushed.
    {ok, DefaultSpaceId} = oz_users:get_default_space(get_user_rest_auth()),
    DefaultSpaceId.
%%    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(DefaultSpaceId),
%%    g_session:put_value(?DEFAULT_SPACE_KEY, DefaultSpaceDirId),
%%    op_gui_utils:register_backend(?MODULE, self()),


%%    SessionId = g_session:get_session_id(),
%%    {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),
%%    #auth{macaroon = Mac, disch_macaroons = DMacs} = Auth,
%%    {ok, DefaultSpace} = oz_users:get_default_space({user, {Mac, DMacs}}),
%%    ?dump(<<"/spaces/", DefaultSpace/binary>>),
%%    {ok, #file_attr{uuid = DefaultSpaceId}} = logical_file_manager:stat(
%%        SessionId, {path, <<"/spaces/", DefaultSpace/binary>>}),
%%    DefaultSpaceId.


%%--------------------------------------------------------------------
%% @doc
%% @todo temporal solution - until events are used in GUI
%% Registers that a pid handles given data backend. This is needed so that we
%% know to whom should data-model updates be pushed.
%% @end
%%--------------------------------------------------------------------
-spec register_backend(Module :: module(), Pid :: pid()) -> ok.
register_backend(Module, Pid) ->
    Diff = fun(#session{data_backend_connections = Cons} = ExistingSess) ->
        ConsPerModule = proplists:get_value(Module, Cons, []),
        ConsWithoutModule = proplists:delete(Module, Cons),
        NewCons = [{Module, [Pid | ConsPerModule]} | ConsWithoutModule],
        {ok, ExistingSess#session{data_backend_connections = NewCons}}
    end,
    SessId = g_session:get_session_id(),
    {ok, SessId} = session:update(SessId, Diff),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Unregisters a pid - it no longer should receive updates.
%% @todo temporal solution - until events are used in GUI
%% @end
%%--------------------------------------------------------------------
-spec unregister_backend(Module :: module(), Pid :: pid()) -> ok.
unregister_backend(Module, Pid) ->
    Diff = fun(#session{data_backend_connections = Cons} = ExistingSess) ->
        ConsPerModule = proplists:get_value(Module, Cons, []),
        ConsWithoutModule = proplists:delete(Module, Cons),
        NewCons = [{Module, ConsPerModule -- [Pid]} | ConsWithoutModule],
        {ok, ExistingSess#session{data_backend_connections = NewCons}}
    end,
    SessId = g_session:get_session_id(),
    {ok, SessId} = session:update(SessId, Diff),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% @todo temporal solution - until events are used in GUI
%% Returns all pids that handle given backend. All GUI sessions are listed and
%% then all pids per session are accumulated.
%% @end
%%--------------------------------------------------------------------
-spec get_all_backend_pids(Module :: module()) -> ok.
get_all_backend_pids(Module) ->
    {ok, Res} = session:all_gui_sessions(),
    AllSessions = lists:map(
        fun(#document{key = Key}) ->
            Key
        end, Res),
    lists:foldl(
        fun(SessionId, Acc) ->
            {ok, #document{value = #session{data_backend_connections = Pids}}}
                = session:get(SessionId),
            PidsByMod = proplists:get_value(Module, Pids, []),
            PidsBySession = proplists:get_value(SessionId, Acc, []),
            NewPidsBySession = lists:usort(PidsByMod ++ PidsBySession),
            [{SessionId, NewPidsBySession} | proplists:delete(SessionId, Acc)]
        end, [], AllSessions).


