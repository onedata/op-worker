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

%% Context definition
-record(fslogic_context, {
    session :: #session{},
    session_id :: undefined | session:id(),
    space_id :: file_meta:uuid(),
%%    user_root_dir :: file_meta:path(),
    share_id :: undefined | od_share:id() %todo TL remove it from here
}).

-type ctx() :: #fslogic_context{}.

%% API
-export([new/1, new/2, set_space_id/2, set_session_id/2, get_user_id/1, get_session_id/1,
    get_auth/1, get_space_id/1, get_share_id/1]).

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
%% Returns newly created fslogic CTX for given session ID, and spaceId
%% @end
%%--------------------------------------------------------------------
-spec new(session:id(), od_space:id() | undefined) -> ctx() | no_return().
new(SessId, SpaceId) ->
    {ok, #document{value = Session}} = session:get(SessId),
    #fslogic_context{session = Session, session_id = SessId, space_id = SpaceId}.

%todo TL use it
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Set user_root_dir in request's context
%%%% @end
%%%%--------------------------------------------------------------------
%%-spec set_user_root_dir(ctx(), file_meta:path()) -> ctx().
%%set_user_root_dir(Ctx, UserRootDir) ->
%%    Ctx#fslogic_context{user_root_dir = UserRootDir}.

%%--------------------------------------------------------------------
%% @doc
%% Set session_id in request's context
%% @end
%%--------------------------------------------------------------------
-spec set_session_id(ctx(), file_meta:path()) -> ctx().
set_session_id(Ctx, SessId) ->
    {ok, #document{value = Session}} = session:get(SessId),
    Ctx#fslogic_context{session_id = SessId, session = Session}.

%%--------------------------------------------------------------------
%% @doc
%% Sets space ID and share ID in fslogic context based on given file.
%% @end
%%--------------------------------------------------------------------
-spec set_space_id(#fslogic_context{},fslogic_worker:ext_file()) ->
    #fslogic_context{}.
set_space_id(#fslogic_context{} = CTX, {guid, FileGUID}) ->
    case fslogic_uuid:unpack_share_guid(FileGUID) of
        {FileUUID, undefined, ShareId} ->
            set_space_id(CTX#fslogic_context{share_id = ShareId}, {uuid, FileUUID});
        {_, SpaceId, ShareId} ->
            CTX#fslogic_context{space_id = SpaceId, share_id = ShareId}
    end;
set_space_id(#fslogic_context{} = CTX, Entry) ->
    case catch fslogic_spaces:get_space(Entry, fslogic_context:get_user_id(CTX)) of
        {not_a_space, _} ->
            CTX#fslogic_context{space_id = undefined};
        {ok, #document{key = SpaceUUID}} ->
            CTX#fslogic_context{space_id = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)}
    end.

%todo TL use this instead of the previous one
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Sets space ID in fslogic context based on given file.
%%%% @end
%%%%--------------------------------------------------------------------
%%-spec set_space_id(ctx(), file_meta:entry() | {guid, fslogic_worker:file_guid()}) -> ctx().
%%set_space_id(#fslogic_context{} = CTX, {guid, FileGUID}) ->
%%    case fslogic_uuid:unpack_guid(FileGUID) of
%%        {FileUUID, undefined} -> set_space_id(CTX, {uuid, FileUUID});
%%        {_, SpaceId} ->
%%            CTX#fslogic_context{space_id = SpaceId}
%%    end;
%%set_space_id(#fslogic_context{} = CTX, Entry) ->
%%    case catch fslogic_spaces:get_space(Entry, fslogic_context:get_user_id(CTX)) of
%%        {not_a_space, _} ->
%%            CTX#fslogic_context{space_id = undefined};
%%        {ok, #document{key = SpaceUUID}} ->
%%            CTX#fslogic_context{space_id = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)}
%%    end.

%%--------------------------------------------------------------------
%% @doc Retrieves user ID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(ctx()) -> od_user:id().
get_user_id(#fslogic_context{session = #session{identity = #user_identity{user_id = UserId}}}) ->
    UserId.

%%--------------------------------------------------------------------
%% @doc Retrieves SessionID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#fslogic_context{session_id = SessionId}) ->
    SessionId.

%%--------------------------------------------------------------------
%% @doc Retrieves session's auth from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_auth(ctx()) -> session:auth().
get_auth(#fslogic_context{session = #session{auth = Auth}}) ->
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
-spec get_share_id(ctx()) -> undeined | od_share:id(). %todo TL remove it from here
get_share_id(#fslogic_context{share_id = ShareId}) ->
    ShareId.

