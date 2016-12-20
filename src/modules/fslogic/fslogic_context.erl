%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides and manages fslogic context information
%%% such user's credentials.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_context).
-author("Rafal Slota").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% Context definition
-record(fslogic_context, {
    session :: session:doc(),
    space_id :: file_meta:uuid(),
    user_root_dir_uuid :: file_meta:uuid(),
    user :: od_user:doc(),
    share_id :: undefined | od_share:id() %todo TL remove it from here
}).

-type ctx() :: #fslogic_context{}.

%% API
-export([new/1, new/2]).
-export([set_space_id/2, set_session_id/2]).
-export([get_user_root_dir_uuid/1, get_user/1]).
-export([get_user_id/1, get_session_id/1, get_auth/1, get_space_id/1,
    get_share_id/1]).
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv new(SessId, undefined).
%%--------------------------------------------------------------------
-spec new(session:id()) -> ctx() | no_return().
new(SessId) ->
    new(SessId, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Returns newly created fslogic Ctx for given session ID, and spaceId
%% @end
%%--------------------------------------------------------------------
-spec new(session:id(), od_space:id() | undefined) -> ctx() | no_return().
new(SessId, SpaceId) ->
    {ok, Session} = session:get(SessId),
    #fslogic_context{session = Session, space_id = SpaceId}.

%%--------------------------------------------------------------------
%% @doc
%% Set session_id in request's context
%% @end
%%--------------------------------------------------------------------
-spec set_session_id(ctx(), file_meta:path()) -> ctx().
set_session_id(Ctx, SessId) ->
    {ok, Session} = session:get(SessId),
    Ctx#fslogic_context{session = Session}.

%%--------------------------------------------------------------------
%% @doc
%% Sets space ID and share ID in fslogic context based on given file.
%% @end
%%--------------------------------------------------------------------
-spec set_space_id(#fslogic_context{},fslogic_worker:ext_file()) ->
    #fslogic_context{}.
set_space_id(#fslogic_context{} = Ctx, {guid, FileGUID}) ->
    case fslogic_uuid:unpack_share_guid(FileGUID) of
        {FileUUID, undefined, ShareId} ->
            set_space_id(Ctx#fslogic_context{share_id = ShareId}, {uuid, FileUUID});
        {_, SpaceId, ShareId} ->
            Ctx#fslogic_context{space_id = SpaceId, share_id = ShareId}
    end;
set_space_id(#fslogic_context{} = Ctx, Entry) ->
    case catch fslogic_spaces:get_space(Entry, fslogic_context:get_user_id(Ctx)) of
        {not_a_space, _} ->
            Ctx#fslogic_context{space_id = undefined};
        {ok, #document{key = SpaceUUID}} ->
            Ctx#fslogic_context{space_id = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)}
    end.

%todo TL use this instead of the previous one
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Sets space ID in fslogic context based on given file.
%%%% @end
%%%%--------------------------------------------------------------------
%%-spec set_space_id(ctx(), file_meta:entry() | {guid, fslogic_worker:file_guid()}) -> ctx().
%%set_space_id(#fslogic_context{} = Ctx, {guid, FileGUID}) ->
%%    case fslogic_uuid:unpack_guid(FileGUID) of
%%        {FileUUID, undefined} -> set_space_id(Ctx, {uuid, FileUUID});
%%        {_, SpaceId} ->
%%            Ctx#fslogic_context{space_id = SpaceId}
%%    end;
%%set_space_id(#fslogic_context{} = Ctx, Entry) ->
%%    case catch fslogic_spaces:get_space(Entry, fslogic_context:get_user_id(Ctx)) of
%%        {not_a_space, _} ->
%%            Ctx#fslogic_context{space_id = undefined};
%%        {ok, #document{key = SpaceUUID}} ->
%%            Ctx#fslogic_context{space_id = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)}
%%    end.

%%--------------------------------------------------------------------
%% @doc
%% Get user_root_dir from request's context
%% @end
%%--------------------------------------------------------------------
-spec get_user_root_dir_uuid(ctx()) -> {file_meta:uuid(), ctx()}.
get_user_root_dir_uuid(Ctx = #fslogic_context{
    user_root_dir_uuid = undefined,
    session = #document{value = #session{identity = #user_identity{user_id = UserId}}}
}) ->
    UserRootDirUuid = fslogic_uuid:user_root_dir_uuid(UserId),
    {UserRootDirUuid, Ctx#fslogic_context{user_root_dir_uuid = UserRootDirUuid}};
get_user_root_dir_uuid(Ctx) ->
    {Ctx#fslogic_context.user_root_dir_uuid, Ctx}.

%%--------------------------------------------------------------------
%% @doc
%% Get user from request's context
%% @end
%%--------------------------------------------------------------------
-spec get_user(ctx()) -> {od_user:doc(), ctx()}.
get_user(Ctx = #fslogic_context{
    user = undefined,
    session = #document{value = #session{auth = Auth, identity = #user_identity{user_id = UserId}}}
}) ->
    {ok, User} = od_user:get_or_fetch(Auth, UserId),
    {User, Ctx#fslogic_context{user = User}};
get_user(Ctx) ->
    {Ctx#fslogic_context.user, Ctx}.


%%--------------------------------------------------------------------
%% @doc Retrieves user ID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(ctx()) -> od_user:id().
get_user_id(#fslogic_context{session = #document{value = #session{identity = #user_identity{user_id = UserId}}}}) ->
    UserId.

%%--------------------------------------------------------------------
%% @doc Retrieves SessionID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#fslogic_context{session = #document{key = SessId}}) ->
    SessId.

%%--------------------------------------------------------------------
%% @doc Retrieves session's auth from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_auth(ctx()) -> session:auth().
get_auth(#fslogic_context{session = #document{value = #session{auth = Auth}}}) ->
    Auth.

%%--------------------------------------------------------------------
%% @doc Retrieves space_id from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(ctx()) -> file_meta:uuid(). %todo TL do something with type
get_space_id(#fslogic_context{space_id = SpaceId}) ->
    SpaceId.

%%--------------------------------------------------------------------
%% @doc Retrieves share_id from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(ctx()) -> undeined | od_share:id(). %todo TL remove it from here and use file_info instead
get_share_id(#fslogic_context{share_id = ShareId}) ->
    ShareId.

