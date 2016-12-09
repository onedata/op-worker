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
-export([get_user_id/1, get_session_id/1, new/1, set_space_and_share_id/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns newly created fslogic CTX for given session ID.
%% @end
%%--------------------------------------------------------------------
-spec new(session:id()) -> fslogic_worker:ctx() | no_return().
new(SessId) ->
    {ok, #document{value = Session}} = session:get(SessId),
    #fslogic_ctx{session = Session, session_id = SessId}.

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec set_user_root_dir(#fslogic_ctx{}) -> term().
set_user_root_dir(_) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Sets space ID and share ID in fslogic context based on given file.
%% @end
%%--------------------------------------------------------------------
-spec set_space_and_share_id(#fslogic_ctx{},fslogic_worker:ext_file()) ->
    #fslogic_ctx{}.
set_space_and_share_id(#fslogic_ctx{} = CTX, {guid, FileGUID}) ->
    case fslogic_uuid:unpack_share_guid(FileGUID) of
        {FileUUID, undefined, ShareId} ->
            set_space_and_share_id(CTX#fslogic_ctx{share_id = ShareId}, {uuid, FileUUID});
        {_, SpaceId, ShareId} ->
            CTX#fslogic_ctx{space_id = SpaceId, share_id = ShareId}
    end;
set_space_and_share_id(#fslogic_ctx{} = CTX, Entry) ->
    case catch fslogic_spaces:get_space(Entry, fslogic_context:get_user_id(CTX)) of
        {not_a_space, _} ->
            CTX#fslogic_ctx{space_id = undefined};
        {ok, #document{key = SpaceUUID}} ->
            CTX#fslogic_ctx{space_id = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)}
    end.

%%--------------------------------------------------------------------
%% @doc Retrieves user ID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(Ctx :: fslogic_worker:ctx()) -> UserId :: od_user:id().
get_user_id(#fslogic_ctx{session = #session{identity = #user_identity{user_id = UserId}}}) ->
    UserId.

%%--------------------------------------------------------------------
%% @doc Retrieves SessionID from fslogic context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(Ctx :: fslogic_worker:ctx()) -> session:id().
get_session_id(#fslogic_ctx{session_id = SessionId}) ->
    SessionId.
