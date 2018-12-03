%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(session_utils).
-author("Michal Wrzeszcz").

-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([is_special/1, is_root/1, is_guest/1]).
-export([root_session_id/0, get_rest_session_id/1, session_ttl/0]).

-define(SESSION_TTL,
    application:get_env(op_worker, gui_session_ttl_seconds, 3600)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if session is of special type: root or guest.
%% @end
%%--------------------------------------------------------------------
-spec is_special(session:id()) -> boolean().
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
-spec is_root(session:id()) -> boolean().
is_root(?ROOT_SESS_ID) ->
    true;
is_root(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if session is of guest type.
%% @end
%%--------------------------------------------------------------------
-spec is_guest(session:id()) -> boolean().
is_guest(?GUEST_SESS_ID) ->
    true;
is_guest(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Returns root session id
%% @end
%%--------------------------------------------------------------------
-spec root_session_id() -> session:id().
root_session_id() ->
    ?ROOT_SESS_ID.

%%--------------------------------------------------------------------
%% @doc
%% Returns rest session id for given identity.
%% @end
%%--------------------------------------------------------------------
-spec get_rest_session_id(session:identity()) -> session:id().
get_rest_session_id(#user_identity{user_id = Uid}) ->
    <<(oneprovider:get_id())/binary, "_", Uid/binary, "_rest_session">>.

%%--------------------------------------------------------------------
%% @doc
%% Returns session Time To Live in seconds.
%% @end
%%--------------------------------------------------------------------
-spec session_ttl() -> integer().
session_ttl() ->
    ?SESSION_TTL.