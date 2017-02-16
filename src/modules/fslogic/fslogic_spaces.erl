%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for managing spaces.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_spaces).
-author("Krzysztof Trzepla").
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([get_space/1, get_space_id/1, get_space_id/2,
    make_space_exist/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(Entry :: {uuid, file_meta:uuid()} | {guid, fslogic_worker:file_guid()}) ->
    SpaceId :: binary().
get_space_id({uuid, FileUUID}) ->
    {ok, #document{key = SpaceUUID}} = get_space({uuid, FileUUID}),
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID);
get_space_id({guid, FileGUID}) ->
    case fslogic_uuid:unpack_guid(FileGUID) of
        {FileUUID, undefined} ->
            get_space_id({uuid, FileUUID});
        {_, SpaceId} ->
            SpaceId
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file path.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(UserCtx :: user_ctx:ctx(), FilePath :: file_meta:path()) ->
    SpaceId :: binary().
get_space_id(UserCtx, Path) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case fslogic_path:get_canonical_file_entry(UserCtx, Tokens) of
        {path, P} = FileEntry ->
            {ok, Tokens1} = fslogic_path:tokenize_skipping_dots(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | _] ->
                    SpaceId;
                _ ->
                    throw({not_in_space, FileEntry})
            end;
        OtherFileEntry ->
            throw({not_in_space, OtherFileEntry})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns space document for given file or space id. Note: This function
%% works only with absolute, user independent paths (cannot be used
%% with paths to default space).
%% @end
%%--------------------------------------------------------------------
-spec get_space(FileEntry :: fslogic_worker:file() | {guid, fslogic_worker:file_guid()}|
    {id, SpaceId :: datastore:id()}) ->
    {ok, ScopeDoc :: datastore:document()} | {error, Reason :: term()}.
get_space({id, SpaceId}) ->
    SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    case file_meta:get({uuid, SpaceUUID}) of
        {error, {not_found, _}} ->
            make_space_exist(SpaceId),
            file_meta:get({uuid, SpaceUUID});
        Res -> Res
    end;
get_space({guid, FileGUID}) ->
    case fslogic_uuid:unpack_guid(FileGUID) of
        {FileUUID, undefined} -> get_space({uuid, FileUUID});
        {_, SpaceId} ->
            get_space({id, SpaceId})
    end;
get_space(FileEntry) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),

    case FileUUID of
        <<"">> ->
            throw({not_a_space, FileEntry});
        _ ->
            {ok, #document{key = SpaceUUID}} = Res = file_meta:get_scope({uuid, FileUUID}),
            _ = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID), %% Crash if given UUID is not a space
            Res

    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates file meta entry for space if not exists
%% @end
%%--------------------------------------------------------------------
-spec make_space_exist(SpaceId :: datastore:id()) -> no_return().
make_space_exist(SpaceId) ->
    CTime = erlang:system_time(seconds),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    case file_meta:exists({uuid, SpaceDirUuid}) of
        true ->
            file_meta:fix_parent_links({uuid, ?ROOT_DIR_UUID},
                {uuid, SpaceDirUuid});
        false ->
            case file_meta:create({uuid, ?ROOT_DIR_UUID},
                #document{key = SpaceDirUuid,
                    value = #file_meta{
                        name = SpaceId, type = ?DIRECTORY_TYPE,
                        mode = 8#1775, owner = ?ROOT_USER_ID, is_scope = true
                    }}) of
                {ok, _} ->
                    case times:create(#document{key = SpaceDirUuid, value =
                        #times{mtime = CTime, atime = CTime, ctime = CTime}}) of
                        {ok, _} -> ok;
                        {error, already_exists} -> ok
                    end;
                {error, already_exists} ->
                    ok
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================