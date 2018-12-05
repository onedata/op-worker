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
-export([is_special/1, is_root/1, is_guest/1, is_provider_session_id/1]).
-export([root_session_id/0, get_rest_session_id/1,
    get_provider_session_id/2, session_id_to_provider_id/1]).
-export([session_ttl/0]).

-define(PROVIDER_SESSION_PREFIX, "$$PRV$$__").

-define(SESSION_TTL,
    application:get_env(op_worker, gui_session_ttl_seconds, 3600)).

%%%===================================================================
%%% API - session type check
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
%% Checks if given session belongs to provider.
%% @end
%%--------------------------------------------------------------------
-spec is_provider_session_id(session:id()) -> boolean().
is_provider_session_id(<<?PROVIDER_SESSION_PREFIX, _SessId/binary>>) ->
    true;
is_provider_session_id(_) ->
    false.

%%%===================================================================
%%% API - session ids
%%%===================================================================

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
%% Generates session id for given provider.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_session_id(Type :: incoming | outgoing, oneprovider:id()) -> session:id().
get_provider_session_id(Type, ProviderId) ->
    <<?PROVIDER_SESSION_PREFIX, (http_utils:base64url_encode(term_to_binary({Type, provider_incoming, ProviderId})))/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Gets provider's id from given session (assumes that the session was created for provider).
%% @end
%%--------------------------------------------------------------------
-spec session_id_to_provider_id(session:id()) -> oneprovider:id().
session_id_to_provider_id(<<?PROVIDER_SESSION_PREFIX, SessId/binary>>) ->
    {_, _, ProviderId} = binary_to_term(http_utils:base64url_decode(SessId)),
    ProviderId.

%%%===================================================================
%%% API - other functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns session Time To Live in seconds.
%% @end
%%--------------------------------------------------------------------
-spec session_ttl() -> integer().
session_ttl() ->
    ?SESSION_TTL.