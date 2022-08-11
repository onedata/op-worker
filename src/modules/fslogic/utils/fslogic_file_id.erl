%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Conversions between paths and uuids and operations on referenced uuids.
%%% Referenced uuid is term connected with links. It is uuid of file on
%%% which link points (or simply file uuid for regular file).
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_file_id).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").

-export([root_dir_guid/0, user_root_dir_uuid/1, user_root_dir_guid/1]).
-export([spaceid_to_space_dir_uuid/1, spaceid_to_space_dir_guid/1, space_dir_uuid_to_spaceid/1]).
-export([spaceid_to_trash_dir_uuid/1, spaceid_to_trash_dir_guid/1]).
-export([shareid_to_share_root_dir_uuid/1, shareid_to_share_root_dir_guid/2, share_root_dir_uuid_to_shareid/1]).
-export([is_special_uuid/1, is_special_guid/1]).
-export([is_root_dir_guid/1, is_root_dir_uuid/1, is_user_root_dir_uuid/1]).
-export([is_space_dir_uuid/1, is_space_dir_guid/1]).
-export([is_trash_dir_uuid/1, is_trash_dir_guid/1]).
-export([is_share_root_dir_uuid/1, is_share_root_dir_guid/1]).
-export([uuid_to_path/2, uuid_to_guid/1]).
-export([is_space_owner/1, unpack_space_owner/1]).
-export([gen_link_uuid/1, is_link_uuid/1, ensure_referenced_uuid/1, ensure_referenced_guid/1]).
-export([gen_symlink_uuid/0, is_symlink_uuid/1, is_symlink_guid/1]).

-define(USER_ROOT_PREFIX, "userRoot_").
-define(SPACE_ROOT_PREFIX, "space_").
-define(ROOT_DIR_VIRTUAL_SPACE_ID, <<"rootDirVirtualSpaceId">>).
-define(SHARE_ROOT_DIR_UUID_PREFIX, "share_").
% Macros for hard links (link is equal to hardlink - see file_meta_hardlinks.erl)
-define(LINK_UUID_PREFIX, "link_").
-define(LINK_UUID_SEPARATOR, "_file_").
-define(LINK_UUID_RAND_PART_BYTES, 8).
% Macro for symlinks
-define(SYMLINK_UUID_PREFIX, "smlnk_").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns Guid of root directory.
%% It is a special directory that is parent for all spaces.
%% @end
%%--------------------------------------------------------------------
-spec root_dir_guid() -> fslogic_worker:file_guid().
root_dir_guid() ->
    file_id:pack_guid(?GLOBAL_ROOT_DIR_UUID, ?ROOT_DIR_VIRTUAL_SPACE_ID).

%%--------------------------------------------------------------------
%% @doc Returns Uuid of user's root directory.
%% @end
%%--------------------------------------------------------------------
-spec user_root_dir_uuid(UserId :: od_user:id()) -> file_meta:uuid().
user_root_dir_uuid(UserId) ->
    <<?USER_ROOT_PREFIX, UserId/binary>>.

%%--------------------------------------------------------------------
%% @doc Returns Guid of user's root directory.
%% @end
%%--------------------------------------------------------------------
-spec user_root_dir_guid(UserId :: od_user:id()) -> fslogic_worker:file_guid().
user_root_dir_guid(UserId) ->
    file_id:pack_guid(user_root_dir_uuid(UserId), ?ROOT_DIR_VIRTUAL_SPACE_ID).


-spec spaceid_to_space_dir_uuid(od_space:id()) -> file_meta:uuid().
spaceid_to_space_dir_uuid(SpaceId) ->
    <<?SPACE_ROOT_PREFIX, SpaceId/binary>>.


-spec spaceid_to_space_dir_guid(od_space:id()) -> fslogic_worker:file_guid().
spaceid_to_space_dir_guid(SpaceId) ->
    file_id:pack_guid(spaceid_to_space_dir_uuid(SpaceId), SpaceId).


-spec space_dir_uuid_to_spaceid(file_meta:uuid()) -> od_space:id().
space_dir_uuid_to_spaceid(<<?SPACE_ROOT_PREFIX, SpaceId/binary>>) ->
    SpaceId.


-spec spaceid_to_trash_dir_uuid(od_space:id()) -> file_meta:uuid().
spaceid_to_trash_dir_uuid(SpaceId) ->
    ?TRASH_DIR_UUID(SpaceId).


-spec spaceid_to_trash_dir_guid(od_space:id()) -> file_id:file_guid().
spaceid_to_trash_dir_guid(SpaceId) ->
    file_id:pack_guid(spaceid_to_trash_dir_uuid(SpaceId), SpaceId).


-spec shareid_to_share_root_dir_uuid(od_share:id()) -> file_meta:uuid().
shareid_to_share_root_dir_uuid(ShareId) ->
    <<?SHARE_ROOT_DIR_UUID_PREFIX, ShareId/binary>>.


-spec shareid_to_share_root_dir_guid(od_share:id(), od_space:id()) -> file_id:file_guid().
shareid_to_share_root_dir_guid(ShareId, SpaceId) ->
    file_id:pack_share_guid(shareid_to_share_root_dir_uuid(ShareId), SpaceId, ShareId).


-spec share_root_dir_uuid_to_shareid(file_meta:uuid()) -> od_share:id().
share_root_dir_uuid_to_shareid(<<?SHARE_ROOT_DIR_UUID_PREFIX, ShareId/binary>>) ->
    ShareId.


-spec is_special_uuid(file_meta:uuid()) -> boolean().
is_special_uuid(FileUuid) ->
    is_root_dir_uuid(FileUuid)
        orelse is_space_dir_uuid(FileUuid)
        orelse is_trash_dir_uuid(FileUuid)
        orelse is_share_root_dir_uuid(FileUuid).


-spec is_special_guid(file_id:file_guid()) -> boolean().
is_special_guid(FileGuid) ->
    is_special_uuid(file_id:guid_to_uuid(FileGuid)).


-spec is_root_dir_guid(file_id:file_guid()) -> boolean().
is_root_dir_guid(FileGuid) ->
    is_root_dir_uuid(file_id:guid_to_uuid(FileGuid)).


-spec is_root_dir_uuid(FileUuid :: file_meta:uuid()) -> boolean().
is_root_dir_uuid(?GLOBAL_ROOT_DIR_UUID) ->
    true;
is_root_dir_uuid(FileUuid) ->
    is_user_root_dir_uuid(FileUuid).


-spec is_user_root_dir_uuid(FileUuid :: file_meta:uuid()) -> boolean().
is_user_root_dir_uuid(FileUuid) ->
    case FileUuid of
        <<?USER_ROOT_PREFIX, _UserId/binary>> ->
            true;
        _ ->
            false
    end.


-spec is_space_dir_uuid(file_meta:uuid()) -> boolean().
is_space_dir_uuid(<<?SPACE_ROOT_PREFIX, _SpaceId/binary>>) -> true;
is_space_dir_uuid(_) -> false.


-spec is_space_dir_guid(file_id:file_guid()) -> boolean().
is_space_dir_guid(FileGuid) ->
    is_space_dir_uuid(file_id:guid_to_uuid(FileGuid)).


-spec is_trash_dir_uuid(file_meta:uuid()) -> boolean().
is_trash_dir_uuid(<<?TRASH_DIR_UUID_PREFIX, _SpaceId/binary>>) -> true;
is_trash_dir_uuid(_) -> false.


-spec is_trash_dir_guid(file_id:file_guid()) -> boolean().
is_trash_dir_guid(FileGuid) ->
    is_trash_dir_uuid(file_id:guid_to_uuid(FileGuid)).


-spec is_share_root_dir_uuid(file_meta:uuid()) -> boolean().
is_share_root_dir_uuid(<<?SHARE_ROOT_DIR_UUID_PREFIX, _ShareId/binary>>) -> true;
is_share_root_dir_uuid(_) -> false.


-spec is_share_root_dir_guid(file_id:file_guid()) -> boolean().
is_share_root_dir_guid(FileGuid) ->
    is_share_root_dir_uuid(file_id:guid_to_uuid(FileGuid)).


%%--------------------------------------------------------------------
%% @doc
%% Gets full file path.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_path(session:id(), file_meta:uuid()) -> file_meta:path().
uuid_to_path(SessionId, FileUuid) ->
    {ok, UserId} = session:get_user_id(SessionId),
    UserRoot = user_root_dir_uuid(UserId),
    case FileUuid of
        UserRoot -> <<"/">>;
        _ ->
            {ok, Path} = gen_path({uuid, FileUuid}, SessionId, []),
            Path
    end.

%%--------------------------------------------------------------------
%% @doc
%% For given file Uuid generates file's Guid. SpaceId is calculated in process.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_guid(file_meta:uuid()) -> fslogic_worker:file_guid().
uuid_to_guid(FileUuid) ->
    SpaceId = uuid_to_space_id(FileUuid),
    file_id:pack_guid(FileUuid, SpaceId).


-spec is_space_owner(od_user:id()) -> boolean().
is_space_owner(<<?SPACE_OWNER_PREFIX_STR, _SpaceId/binary>>) ->
    true;
is_space_owner(_) ->
    false.


-spec unpack_space_owner(od_user:id()) -> {ok, od_space:id()} | {error, term()}.
unpack_space_owner(<<?SPACE_OWNER_PREFIX_STR, SpaceId/binary>>) ->
    {ok, SpaceId};
unpack_space_owner(_) ->
    {error, not_space_owner}.


-spec gen_link_uuid(file_meta:uuid()) -> file_meta_hardlinks:link().
gen_link_uuid(FileUuid) ->
    RandPart = str_utils:rand_hex(?LINK_UUID_RAND_PART_BYTES),
    <<?LINK_UUID_PREFIX, RandPart/binary, ?LINK_UUID_SEPARATOR, FileUuid/binary>>.

-spec is_link_uuid(file_meta:uuid() | file_meta_hardlinks:link()) -> boolean().
is_link_uuid(<<?LINK_UUID_PREFIX, _/binary>>) -> true;
is_link_uuid(_) -> false.

%%--------------------------------------------------------------------
%% @doc Returns referenced uuid
%% (file uuid for regular file or uuid of file on which link points).
%% @end
%%--------------------------------------------------------------------
-spec ensure_referenced_uuid(file_meta:uuid() | file_meta_hardlinks:link()) -> file_meta:uuid().
ensure_referenced_uuid(<<?LINK_UUID_PREFIX, UuidTail/binary>>) ->
    [_, FileUuid] = binary:split(UuidTail, <<?LINK_UUID_SEPARATOR>>),
    FileUuid;
ensure_referenced_uuid(Uuid) ->
    Uuid.


-spec ensure_referenced_guid(file_id:file_guid()) -> file_id:file_guid().
ensure_referenced_guid(Guid) ->
    {Uuid, SpaceId} = file_id:unpack_guid(Guid),
    ReferencedUuid = ensure_referenced_uuid(Uuid),
    file_id:pack_guid(ReferencedUuid, SpaceId).


-spec gen_symlink_uuid() -> file_meta:uuid().
gen_symlink_uuid() ->
    RandPart = datastore_key:new(),
    <<?SYMLINK_UUID_PREFIX, RandPart/binary>>.

-spec is_symlink_uuid(file_meta:uuid()) -> boolean().
is_symlink_uuid(<<?SYMLINK_UUID_PREFIX, _/binary>>) -> true;
is_symlink_uuid(_) -> false.


-spec is_symlink_guid(file_id:file_guid()) -> boolean().
is_symlink_guid(FileGuid) ->
    is_symlink_uuid(file_id:guid_to_uuid(FileGuid)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Internal helper for gen_path/2. Accumulates all file meta names
%% and concatenates them into path().
%% @end
%%--------------------------------------------------------------------
-spec gen_path(file_meta:entry(), session:id(), [file_meta:name()]) ->
    {ok, file_meta:path()} | {error, term()} | no_return().
gen_path(Entry, SessionId, Tokens) ->
    {ok, #document{key = Uuid, value = #file_meta{name = Name}} = Doc} = file_meta:get(Entry),
    case file_meta:get_parent(Doc) of
        {ok, #document{key = ?GLOBAL_ROOT_DIR_UUID}} ->
            SpaceId = fslogic_file_id:space_dir_uuid_to_spaceid(Uuid),
            {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
            {ok, filepath_utils:join([<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens])};
        {ok, #document{key = ParentUuid}} ->
            gen_path({uuid, ParentUuid}, SessionId, [Name | Tokens])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_space_id(file_meta:uuid()) -> SpaceId :: od_space:id().
uuid_to_space_id(FileUuid) ->
    case is_root_dir_uuid(FileUuid) of
        true ->
            ?ROOT_DIR_VIRTUAL_SPACE_ID;
        false ->
            {ok, Doc} = file_meta:get_including_deleted(FileUuid),
            {ok, SpaceId} = file_meta:get_scope_id(Doc),
            SpaceId
    end.
