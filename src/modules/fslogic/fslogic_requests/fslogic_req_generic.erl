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
-export([get_file_path/2, replicate_file/3, check_perms/3, create_share/3, remove_share/2]).
%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Translates given file's UUID to absolute path.
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{} | no_return().
get_file_path(Ctx, {uuid, FileUUID}) ->
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_path{value = fslogic_uuid:uuid_to_path(Ctx, FileUUID)}
    }.

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
%% @equiv replicate_file(Ctx, {uuid, Uuid}, Block, 0)
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_context:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block()) ->
    #provider_response{}.
replicate_file(Ctx, {uuid, Uuid}, Block) ->
    replicate_file(Ctx, {uuid, Uuid}, Block, 0).

%%--------------------------------------------------------------------
%% @doc
%% Replicate given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_context:ctx(), {uuid, file_meta:uuid()},
    fslogic_blocks:block(), non_neg_integer()) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
replicate_file(Ctx, {uuid, Uuid}, Block, Offset) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case file_meta:get({uuid, Uuid}) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
            case dir_req:read_dir(Ctx, file_info:new_by_guid(fslogic_uuid:uuid_to_guid(Uuid)), Offset, Chunk) of
                #fuse_response{fuse_response = #file_children{child_links = ChildLinks}}
                    when length(ChildLinks) < Chunk ->
                    utils:pforeach(
                        fun(#child_link{uuid = ChildGuid}) ->
                            replicate_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ChildGuid)}, Block)
                        end, ChildLinks),
                    #provider_response{status = #status{code = ?OK}};
                #fuse_response{fuse_response = #file_children{child_links = ChildLinks}} ->
                    utils:pforeach(
                        fun(#child_link{uuid = ChildGuid}) ->
                            replicate_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ChildGuid)}, Block)
                        end, ChildLinks),
                    replicate_file(Ctx, {uuid, Uuid}, Block, Offset + Chunk);
                Other ->
                    Other
            end;
        {ok, _} ->
            #fuse_response{status = Status} = fslogic_req_regular:synchronize_block(Ctx, {uuid, Uuid}, Block, false),
            #provider_response{status = Status}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check given permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(fslogic_context:ctx(), {uuid, file_meta:uuid()}, fslogic_worker:open_flag()) ->
    #provider_response{}.
check_perms(Ctx, Uuid, read) ->
    check_perms_read(Ctx, Uuid);
check_perms(Ctx, Uuid, write) ->
    check_perms_write(Ctx, Uuid);
check_perms(Ctx, Uuid, rdwr) ->
    check_perms_rdwr(Ctx, Uuid).

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

%%--------------------------------------------------------------------
%% @doc
%% Check read permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_read(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
check_perms_read(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check write permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_write(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
check_perms_write(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check rdwr permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_rdwr(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
check_perms_rdwr(_Ctx, _Uuid) ->
    #provider_response{status = #status{code = ?OK}}.