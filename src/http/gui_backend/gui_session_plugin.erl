%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements session_logic_behaviour and
%%% is capable of persisting GUI sessions in datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_session_plugin).
-author("Lukasz Opiola").
-behaviour(gui_session_plugin_behaviour).


-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("http/gui_common.hrl").
-include_lib("gui/include/gui.hrl").
-include_lib("ctool/include/logging.hrl").


%% session_logic_behaviour API
-export([init/0, cleanup/0]).
-export([create_session/2, update_session/2, lookup_session/1]).
-export([delete_session/1]).
-export([get_cookie_ttl/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ets:new(?LS_CACHE_ETS, [
        set, public, named_table,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback cleanup/1.
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    ets:delete(?LS_CACHE_ETS),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback create_session/1.
%% @end
%%--------------------------------------------------------------------
-spec create_session(UserId :: term(), CustomArgs :: [term()]) ->
    {ok, SessionId :: binary()} | {error, term()}.
create_session(_UserId, [#user_identity{} = Identity, #macaroon_auth{} = Auth]) ->
    %% UserId no needed here to crete session as it is indicated by Auth.
    case session_manager:create_gui_session(Identity, Auth) of
        {ok, SessionId} -> {ok, SessionId};
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback update_session/2.
%% @end
%%--------------------------------------------------------------------
-spec update_session(SessId :: binary(),
    MemoryUpdateFun :: fun((maps:map()) -> maps:map())) ->
    ok | {error, term()}.
update_session(SessionId, MemoryUpdateFun) ->
    SessionUpdateFun = fun(#session{memory = OldMemory} = Session) ->
        {ok, Session#session{memory = MemoryUpdateFun(OldMemory)}}
    end,
    case session:update(SessionId, SessionUpdateFun) of
        {ok, _} -> ok;
        {error, Error} -> {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback lookup_session/1.
%% @end
%%--------------------------------------------------------------------
-spec lookup_session(SessionId :: binary()) ->
    {ok, Memory :: maps:map()} | undefined.
lookup_session(SessionId) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{memory = Memory}}} -> {ok, Memory};
        _ -> undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback delete_session/1.
%% @end
%%--------------------------------------------------------------------
-spec delete_session(SessionId :: binary()) -> ok | {error, term()}.
delete_session(SessionId) ->
    case session_manager:remove_session(SessionId) of
        ok -> ok;
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_session_plugin_behaviour} callback get_cookie_ttl/0.
%% @end
%%--------------------------------------------------------------------
-spec get_cookie_ttl() -> integer() | {error, term()}.
get_cookie_ttl() ->
    case application:get_env(?APP_NAME, gui_session_ttl_seconds) of
        {ok, Val} when is_integer(Val) -> Val;
        _ -> {error, missing_env}
    end.
