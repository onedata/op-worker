%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on file attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(file_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_file/5, make_file/4]).
-export([save_handle/2]). %todo delete

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Create and open file. Return handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file(fslogic_context:ctx(), ParentFile :: file_info:file_info(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
create_file(Ctx, ParentFile, Name, Mode, _Flag) ->
    {File, ParentFile2} = create_file_doc(Ctx, ParentFile, Name, Mode),
    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = file_info:get_space_id(ParentFile2),
    {{uuid, FileUuid}, File2} = file_info:get_uuid_entry(File),
    {StorageId, FileId} = sfm_utils:create_storage_file(SpaceId, FileUuid, SessId, Mode), %todo pass file_info
    {ParentFileEntry, _ParentFile3} = file_info:get_uuid_entry(ParentFile2),
    fslogic_times:update_mtime_ctime(ParentFileEntry, fslogic_context:get_user_id(Ctx)), %todo pass file_info
    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
    SpaceDirUuid = file_info:get_space_dir_uuid(ParentFile2),
    SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
    {ok, HandleId} = save_handle(SessId, Handle),
    #fuse_response{fuse_response = #file_attr{} = FileAttr} = attr_req:get_file_attr_no_permission_check(Ctx, File2),
    {FileGuid, _File3} = file_info:get_guid(File2),
    FileLocation = #file_location{
        uuid = FileGuid,
        provider_id = oneprovider:get_provider_id(),
        storage_id = StorageId,
        file_id = FileId,
        blocks = [#file_block{offset = 0, size = 0}],
        space_id = SpaceId
    },
    ok = file_handles:register_open(FileUuid, SessId, 1), %todo pass file_info
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_created{
            handle_id = HandleId,
            file_attr = FileAttr,
            file_location = FileLocation
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Create file. Return its attributes.
%% @end
%%--------------------------------------------------------------------
-spec make_file(fslogic_context:ctx(), ParentFile :: file_info:file_info(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
make_file(Ctx, ParentFile, Name, Mode) ->
    {File, ParentFile2} = create_file_doc(Ctx, ParentFile, Name, Mode),

    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = fslogic_context:get_space_id(Ctx),
    {{uuid, FileUuid}, File2} = file_info:get_uuid_entry(File),
    sfm_utils:create_storage_file(SpaceId, FileUuid, SessId, Mode), %todo pass file_info

    {ParentFileEntry, _ParentFile3} = file_info:get_uuid_entry(ParentFile2),
    fslogic_times:update_mtime_ctime(ParentFileEntry, fslogic_context:get_user_id(Ctx)), %todo pass file_info

    attr_req:get_file_attr_no_permission_check(Ctx, File2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create file_meta and times documents for the new file.
%% @end
%%--------------------------------------------------------------------
-spec create_file_doc(fslogic_context:ctx(), file_info:file_info(), file_meta:name(), file_meta:mode()) ->
    {ChildFile :: file_info:file_info(), NewParentFile :: file_info:file_info()}.
create_file_doc(Ctx, ParentFile, Name, Mode)  ->
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(Ctx)
    }},
    {FileEntry, ParentFile2} = file_info:get_uuid_entry(ParentFile),
    {ok, FileUuid} = file_meta:create(FileEntry, File), %todo pass file_info

    CTime = erlang:system_time(seconds),
    {ok, _} = times:create(#document{key = FileUuid, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    SpaceId = file_info:get_space_id(ParentFile2),
    {file_info:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)), ParentFile2}.

%%--------------------------------------------------------------------
%% @doc Saves file handle in user's session, returns id of saved handle
%% @end
%%--------------------------------------------------------------------
-spec save_handle(session:id(), storage_file_manager:handle()) ->
    {ok, binary()}.
save_handle(SessId, Handle) ->
    HandleId = base64:encode(crypto:rand_bytes(20)),
    case session:is_special(SessId) of
        true -> ok;
        _ -> session:add_handle(SessId, HandleId, Handle)
    end,
    {ok, HandleId}.
