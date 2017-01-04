%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic generic (both for regular and special files) request handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_generic).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("modules/events/types.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([create_share/3, remove_share/2]).
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Changes file owner.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chown(fslogic_context:ctx(), File :: fslogic_worker:file(), UserId :: od_user:id()) ->
    #fuse_response{} | no_return().
-check_permissions([{?write_owner, 2}]).
chown(_, _File, _UserId) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.


%%--------------------------------------------------------------------
%% @doc
%% Share file under given uuid
%% @end
%%--------------------------------------------------------------------
-spec create_share(fslogic_context:ctx(), {uuid, file_meta:uuid()}, od_share:name()) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}]).
create_share(Ctx, {uuid, FileUuid}, Name) ->
    SpaceId = fslogic_context:get_space_id(Ctx),
    SessId = fslogic_context:get_session_id(Ctx),
    {ok, Auth} = session:get_auth(SessId),
    ShareId = datastore_utils:gen_uuid(),
    ShareGuid = fslogic_uuid:uuid_to_share_guid(FileUuid, SpaceId, ShareId),
    {ok, _} = share_logic:create(Auth, ShareId, Name, SpaceId, ShareGuid),
    {ok, _} = file_meta:add_share(FileUuid, ShareId),
    #provider_response{status = #status{code = ?OK}, provider_response = #share{share_id = ShareId, share_file_uuid = ShareGuid}}.

%%--------------------------------------------------------------------
%% @doc
%% Share file under given uuid
%% @end
%%--------------------------------------------------------------------
-spec remove_share(fslogic_context:ctx(), {uuid, file_meta:uuid()}) -> #provider_response{}.
-check_permissions([{traverse_ancestors, 2}]).
remove_share(Ctx, {uuid, FileUuid}) ->
    Auth = fslogic_context:get_auth(Ctx),
    ShareId = file_info:get_share_id(Ctx),

    ok = share_logic:delete(Auth, ShareId),
    {ok, _} = file_meta:remove_share(FileUuid, ShareId),
    ok = permissions_cache:invalidate_permissions_cache(file_meta, FileUuid),

    #provider_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
