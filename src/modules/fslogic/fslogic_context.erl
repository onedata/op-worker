%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides and manages fslogic context information
%%%      such user's credentials.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_context).
-author("Rafal Slota").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([gen_global_session_id/2, read_global_session_id/1, is_global_session_id/1]).
-export([get_user_id/1, new/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%  Returns newly created fslogic CTX for given session ID.
%% @end
%%--------------------------------------------------------------------
-spec new(session:id(), file_meta:entry() | {space_id, SpaceId :: file_meta:uuid()}) ->
    fslogic_worker:ctx() | no_return().
new(SessId, {space_id, SpaceId}) ->
    {ok, #document{value = Session}} = session:get(SessId),
    #fslogic_ctx{session = Session, session_id = SessId, space_id = SpaceId};
new(SessId, Entry) ->
    {ok, #document{key = SpaceId}} = file_meta:get_scope(Entry),
    new(SessId, {space_id, SpaceId}).


%% gen_global_session_id/1
%%-------------------------------------------------------------------- 
%% @doc Converts given ProviderId and SessionId into GlobalSessionId that can be recognised by other providers.
%% @end
-spec gen_global_session_id(ProviderId :: iolist(), SessionId :: iolist()) -> GlobalSessionId :: binary() | undefined.
%%-------------------------------------------------------------------- 
gen_global_session_id(_, undefined) ->
    undefined;
gen_global_session_id(ProviderId, SessionId) ->
    ProviderId1 = str_utils:to_binary(ProviderId),
    SessionId1 = str_utils:to_binary(SessionId),
    <<ProviderId1/binary, "::", SessionId1/binary>>.


%%--------------------------------------------------------------------
%% @doc Converts given GlobalSessionId into ProviderId and SessionId. This method inverts gen_global_session_id/1.
%%      Fails with exception if given argument has invalid format.
%% @end
%%--------------------------------------------------------------------
-spec read_global_session_id(GlobalSessionId :: iolist()) -> {ProviderId :: binary(), SessionId :: binary()} | no_return().
read_global_session_id(GlobalSessionId) ->
    GlobalSessionId1 = str_utils:to_binary(GlobalSessionId),
    [ProviderId, SessionId] = binary:split(GlobalSessionId1, <<"::">>),
    {ProviderId, SessionId}.


%%-------------------------------------------------------------------- 
%% @doc Checks if given SessionId can be recognised by other providers (i.e. was generated by gen_global_session_id/1).
%% @end
%%-------------------------------------------------------------------- 
-spec is_global_session_id(GlobalSessionId :: iolist()) -> boolean().
is_global_session_id(GlobalSessionId) ->
    GlobalSessionId1 = str_utils:to_binary(GlobalSessionId),
    length(binary:split(GlobalSessionId1, <<"::">>)) =:= 2.

%%--------------------------------------------------------------------
%% @doc Retrieves user ID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(Ctx :: fslogic_worker:ctx()) -> UserId :: onedata_user:id().
get_user_id(#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}}) ->
    UserId.
