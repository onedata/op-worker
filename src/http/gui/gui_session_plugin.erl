%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C): 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc: This module implements session_logic_behaviour and exports an
%%% API for persisting GUI sessions.
%%% @end
%%%-------------------------------------------------------------------

-module(gui_session_plugin).
-behaviour(gui_session_plugin_behaviour).


-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("gui/include/gui.hrl").
-include_lib("ctool/include/logging.hrl").


%% session_logic_behaviour API
-export([init/0, cleanup/0]).
-export([create_session/1, update_session/2, lookup_session/1]).
-export([delete_session/1]).
-export([get_cookie_ttl/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Initializes the session_logic module. Any setup such as ets creation
%% should be performed in this function.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc Performs any cleanup, such as deleting the previously created ets tables.
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a new session under SessionId key.
%% The session is valid up to given moment (Expires).
%% Expires is expressed in number of seconds since epoch.
%% CustomArgs are the args that are passed to g_session:log_in/1 function,
%% they are application specific arguments that are needed to create a session.
%% @end
%%--------------------------------------------------------------------
-spec create_session(CustomArgs) ->
    {ok, SessionId} | {error, term()} when
    CustomArgs :: [term()], SessionId :: binary().
create_session([#auth{} = Auth]) ->
    case session_manager:create_gui_session(Auth) of
        {ok, SessionId} ->
            {ok, SessionId};
        {error, Error} ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves session data under SessionID key. Updates the session memory,
%% the entry is valid up to given moment (Expires).
%% If there is no record of session
%% with id SessionID, error atom should be returned.
%% Expires is expressed in number of seconds since epoch.
%% @end
%%--------------------------------------------------------------------
-spec update_session(SessionID, Memory) -> ok | {error, term()}
    when SessionID :: binary(),
    Memory :: [{Key :: binary(), Value :: binary}].
update_session(SessionId, Memory) ->
    case session:update(SessionId, #{memory => Memory}) of
        {ok, _} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Lookups a session by given SessionID key.
%% On success, returns a tuple - expiration time and session memory,
%% or undefined if given session does not exist.
%% @end
%%--------------------------------------------------------------------
-spec lookup_session(SessionId :: binary()) -> {ok, Memory} | undefined
    when Memory :: [{Key :: binary(), Value :: binary}].
lookup_session(SessionId) ->
    case SessionId of
        undefined ->
            undefined;
        _ ->
            case session:get(SessionId) of
                {ok, #document{value = #session{memory = Memory}}} ->
                    {ok, Memory};
                _ ->
                    undefined
            end
    end.


%%--------------------------------------------------------------------
%% @doc Deletes a session by SessionId key.
%% @end
%%--------------------------------------------------------------------
-spec delete_session(SessionID :: binary()) -> ok | {error, term()}.
delete_session(SessionId) ->
    case SessionId of
        undefined ->
            ok;
        _ ->
            case session:delete(SessionId) of
                ok -> ok;
                {error, _} = Error -> Error
            end
    end.


%%--------------------------------------------------------------------
%% @doc Returns cookies time to live in seconds.
%% @end
%%--------------------------------------------------------------------
-spec get_cookie_ttl() -> integer() | {error, term()}.
get_cookie_ttl() ->
    case application:get_env(?APP_NAME, gui_cookie_ttl) of
        {ok, Val} when is_integer(Val) ->
            Val;
        _ ->
            {error, missing_env}
    end.
