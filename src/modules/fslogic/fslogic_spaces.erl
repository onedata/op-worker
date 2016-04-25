%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_spaces).
-author("Krzysztof Trzepla").
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([get_default_space/1, get_default_space_id/1, get_space/2, get_space/1, get_space_id/1, get_space_id/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns default space document.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space(UserIdOrCTX :: fslogic_worker:ctx() | onedata_user:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_default_space(UserIdOrCTX) ->
    {ok, DefaultSpaceId} = get_default_space_id(UserIdOrCTX),
    file_meta:get_space_dir(DefaultSpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns default space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space_id(UserIdOrCTX :: fslogic_worker:ctx() | onedata_user:id()) ->
    {ok, SpaceId :: binary()}.
get_default_space_id(CTX = #fslogic_ctx{}) ->
    UserId = fslogic_context:get_user_id(CTX),
    get_default_space_id(UserId);
get_default_space_id(?ROOT_USER_ID) ->
    throw(no_default_space_for_root_user);
get_default_space_id(UserId) ->
    {ok, #document{value = #onedata_user{space_ids = [DefaultSpaceId | _]}}} =
        onedata_user:get(UserId),
    {ok, DefaultSpaceId}.


%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(FileUUID :: file_meta:uuid()) ->
    SpaceId :: binary().
get_space_id(FileUUID) ->
    {ok, #document{key = SpaceUUID}} = get_space({uuid, FileUUID}),
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID).


%%--------------------------------------------------------------------
%% @doc
%% Returns space ID for given file path.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(CTX :: fslogic_worker:ctx(), FilePath :: file_meta:path()) ->
    SpaceId :: binary().
get_space_id(CTX, Path) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    case fslogic_path:get_canonical_file_entry(CTX, Tokens) of
        {path, P} = FileEntry ->
            {ok, Tokens1} = fslogic_path:verify_file_path(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, ?SPACES_BASE_DIR_NAME, SpaceId | _] ->
                    SpaceId;
                _ ->
                    throw({not_in_space, FileEntry})
            end;
        OtherFileEntry ->
            throw({not_in_space, OtherFileEntry})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns space document for given file. Note: This function works only with
%% absolute, user independent paths (cannot be used with paths to default space).
%% @end
%%--------------------------------------------------------------------
-spec get_space(FileEntry :: fslogic_worker:file() | {guid, fslogic_worker:file_guid()}) ->
    {ok, ScopeDoc :: datastore:document()} | {error, Reason :: term()}.
get_space({guid, FileGUID}) ->
    case fslogic_uuid:unpack_file_guid(FileGUID) of
        {FileUUID, undefined} -> get_space({uuid, FileUUID});
        {_, SpaceId} ->
            file_meta:get(fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId))
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
%% Returns file_meta space document for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_space(FileEntry :: fslogic_worker:file() | {guid, fslogic_worker:file_guid()}, UserId :: onedata_user:id()) ->
    {ok, ScopeDoc :: datastore:document()} | {error, Reason :: term()}.
get_space({guid, FileGUID}, UserId) ->
    case fslogic_uuid:unpack_file_guid(FileGUID) of
        {FileUUID, undefined} -> get_space({uuid, FileUUID}, UserId);
        {_, SpaceId} ->
            file_meta:get(fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId))
    end;
get_space(FileEntry, UserId) ->
    ?info("get_space ~p ~p", [FileEntry, UserId]),
    DefaultSpaceUUID = fslogic_uuid:default_space_uuid(UserId),
    SpacesDir = fslogic_uuid:spaces_uuid(UserId),
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),

    SpaceDocument = case FileUUID of
        <<"">> ->
            throw({not_a_space, FileEntry});
        SpacesDir ->
            throw({not_a_space, FileEntry});
        DefaultSpaceUUID ->
            {ok, DefaultSpace} = get_default_space(UserId),
            DefaultSpace;
        _ ->
            {ok, Doc} = file_meta:get_scope(FileEntry),
            Doc
    end,

    case UserId of
        ?ROOT_USER_ID ->
            {ok, SpaceDocument};
        _ ->
            {ok, SpaceIds} = onedata_user:get_spaces(UserId),
            #document{key = SpaceUUID} = SpaceDocument,
            SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
            case is_list(SpaceIds) andalso lists:member(SpaceId, SpaceIds) of
                true ->
                    {ok, SpaceDocument};
                false -> throw({not_a_space, FileEntry})
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================