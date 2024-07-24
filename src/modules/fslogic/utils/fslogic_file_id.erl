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

-export([uuid_to_guid/1]).
-export([is_space_owner/1, unpack_space_owner/1]).
-export([gen_link_uuid/1, gen_link_uuid/2, gen_deleted_opened_file_link_uuid/1,
    is_link_uuid/1, ensure_referenced_uuid/1, ensure_referenced_guid/1]).
-export([gen_symlink_uuid/0, is_symlink_uuid/1, is_symlink_guid/1]).

% Macros for hard links (link is equal to hardlink - see file_meta_hardlinks.erl)
-define(LINK_UUID_SEPARATOR, "_file_").
-define(LINK_UUID_RAND_PART_BYTES, 8).
% Macro for symlinks
-define(SYMLINK_UUID_PREFIX, "smlnk_").

%%%===================================================================
%%% API
%%%===================================================================

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
    gen_link_uuid(FileUuid, str_utils:rand_hex(?LINK_UUID_RAND_PART_BYTES)).

-spec gen_link_uuid(file_meta:uuid(), binary()) -> file_meta_hardlinks:link().
gen_link_uuid(FileUuid, LinkPart) ->
    <<?LINK_UUID_PREFIX, LinkPart/binary, ?LINK_UUID_SEPARATOR, FileUuid/binary>>.

gen_deleted_opened_file_link_uuid(FileUuid) ->
    gen_link_uuid(FileUuid, ?OPENED_DELETED_FILE_LINK_ID_SEED).

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
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_space_id(file_meta:uuid()) -> SpaceId :: od_space:id().
uuid_to_space_id(FileUuid) ->
    case special_dirs:is_scope_root_dir(FileUuid) of
        true ->
            ?ROOT_DIR_VIRTUAL_SPACE_ID;
        false ->
            {ok, Doc} = file_meta:get_including_deleted(FileUuid),
            {ok, SpaceId} = file_meta:get_scope_id(Doc),
            SpaceId
    end.
