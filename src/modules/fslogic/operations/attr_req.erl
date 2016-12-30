%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests related to file guid
%%% @end
%%%--------------------------------------------------------------------
-module(attr_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_file_attr/2, get_child_attr/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets file's attributes.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(fslogic_context:ctx(), file_info:file_info()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
get_file_attr(Ctx, File) ->
    {FileDoc = #document{key = Uuid, value = #file_meta{
        type = Type, mode = Mode, provider_id = ProviderId, uid = OwnerId,
        shares = Shares}}, File2
    } = file_info:get_file_doc(File),
    ShareId = file_info:get_share_id(File),
    UserId = fslogic_context:get_user_id(Ctx),
    {FileName, Ctx2, File3} = file_info:get_aliased_name(File2, Ctx),
    SpaceId = file_info:get_space_id(File3),
    {#posix_user_ctx{gid = GID, uid = UID}, File4} =
        file_info:get_storage_user_context(File3, Ctx2),
    Size = fslogic_blocks:get_file_size(FileDoc), %todo TL consider caching file_location in File record
    {ParentGuid, File5} = file_info:get_parent_guid(File4, UserId),
    {{ATime, CTime, MTime}, _File6} = file_info:get_times(File5),

    FinalUID = fix_wrong_uid(UID, UserId, OwnerId), %todo see function doc
    #fuse_response{status = #status{code = ?OK}, fuse_response = #file_attr{
        gid = GID, parent_uuid = ParentGuid,
        uuid = fslogic_uuid:uuid_to_share_guid(Uuid, SpaceId, ShareId),
        type = Type, mode = Mode, atime = ATime, mtime = MTime,
        ctime = CTime, uid = FinalUID, size = Size, name = FileName, provider_id = ProviderId,
        shares = Shares, owner_id = OwnerId
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Fetches attributes of directory's child (if exists).
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(fslogic_context:ctx(), ParentFile :: file_info:file_info(),
    Name :: file_meta:name()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?list_container, 2}]).
get_child_attr(Ctx, ParentFile, Name) ->
    UserId = fslogic_context:get_user_id(Ctx),
    {ChildFile, _NewParentFile} = file_info:get_child(ParentFile, Name, UserId),
    attr_req:get_file_attr(Ctx, ChildFile).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @todo TL something is messed up with luma design, if we need to put such code here
%% @doc
%% Not sure why we need it. Remove asap after fixing luma.
%% @end
%%--------------------------------------------------------------------
-spec fix_wrong_uid(posix_user:uid(), UserId :: od_user:id(), FileOwnerId :: od_user:id()) ->
    posix_user:uid().
fix_wrong_uid(Uid, UserId, UserId) ->
    Uid;
fix_wrong_uid(_Uid, _UserId, OwnerId) ->
    luma_utils:gen_storage_uid(OwnerId).