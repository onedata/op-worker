%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Conversions between paths and uuids.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_uuid).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_root_dir_uuid/1, user_root_dir_uuid/1, user_root_dir_guid/1]).
-export([uuid_to_path/2, uuid_to_guid/1]).
-export([spaceid_to_space_dir_uuid/1, space_dir_uuid_to_spaceid/1,
    space_dir_uuid_to_spaceid_no_error/1, spaceid_to_space_dir_guid/1]).

-define(USER_ROOT_PREFIX, "userRoot_").
-define(SPACE_ROOT_PREFIX, "space_").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns true if given uuid represents user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_root_dir_uuid(FileUuid :: file_meta:uuid()) -> boolean().
is_root_dir_uuid(?ROOT_DIR_UUID) ->
    true;
is_root_dir_uuid(FileUuid) ->
    case FileUuid of
        <<?USER_ROOT_PREFIX, _UserId/binary>> ->
            true;
        _ ->
            false
    end.

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
    file_id:pack_guid(user_root_dir_uuid(UserId), undefined).

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
    try uuid_to_space_id(FileUuid) of
        SpaceId ->
            file_id:pack_guid(FileUuid, SpaceId)
    catch
        {not_a_space, _} ->
            file_id:pack_guid(FileUuid, undefined)
    end.

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to uuid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_uuid(od_space:id()) -> file_meta:uuid().
spaceid_to_space_dir_uuid(SpaceId) ->
    <<?SPACE_ROOT_PREFIX, SpaceId/binary>>.

%%--------------------------------------------------------------------
%% @doc Convert SpaceId to guid of file_meta document of this space directory.
%%--------------------------------------------------------------------
-spec spaceid_to_space_dir_guid(od_space:id()) -> fslogic_worker:file_guid().
spaceid_to_space_dir_guid(SpaceId) ->
    file_id:pack_guid(spaceid_to_space_dir_uuid(SpaceId), SpaceId).

%%--------------------------------------------------------------------
%% @doc Convert file_meta uuid of space directory to SpaceId
%%--------------------------------------------------------------------
-spec space_dir_uuid_to_spaceid(file_meta:uuid()) -> od_space:id().
space_dir_uuid_to_spaceid(SpaceUuid) ->
    case SpaceUuid of
        <<?SPACE_ROOT_PREFIX, SpaceId/binary>> ->
            SpaceId;
        _ ->
            throw({not_a_space, {uuid, SpaceUuid}}) %todo remove this throw and return undefined instead
    end.

%%--------------------------------------------------------------------
%% @doc Convert file_meta uuid of space directory to SpaceId
%%--------------------------------------------------------------------
-spec space_dir_uuid_to_spaceid_no_error(file_meta:uuid()) -> od_space:id().
space_dir_uuid_to_spaceid_no_error(SpaceUuid) ->
    try space_dir_uuid_to_spaceid(SpaceUuid) of
        SpaceId ->
            SpaceId
    catch
        _:_ -> <<>>
    end.

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
        {ok, #document{key = ?ROOT_DIR_UUID}} ->
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(Uuid),
            {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, SpaceName | Tokens])};
        {ok, #document{key = ParentUuid}} ->
            gen_path({uuid, ParentUuid}, SessionId, [Name | Tokens])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec uuid_to_space_id(file_meta:uuid()) ->
    SpaceId :: od_space:id().
uuid_to_space_id(FileUuid) ->
    case FileUuid of
        ?ROOT_DIR_UUID ->
            undefined;
        _ ->
            {ok, Doc} = file_meta:get_including_deleted(FileUuid),
            {ok, SpaceUuid} = file_meta:get_scope_id(Doc),
            fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUuid)
    end.
