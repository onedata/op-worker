%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module exports utility tools for fslogic
%% @end
%% ===================================================================
-module(fslogic_utils).


-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").


%% API
-export([random_ascii_lowercase_sequence/1, gen_storage_uid/1, get_parent/1, gen_storage_file_id/1]).
-export([get_local_file_location/1, get_local_file_locations/1, get_local_storage_file_locations/1]).
-export([wait_for_links/2, wait_for_file_meta/2]).
-export([get_storage_type/1, get_storage_id/1, gen_storage_gid/2]).
-export([get_s3_user/2, get_ceph_user/2, get_posix_user/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns StorageType for given StorageId
%% @end
%%--------------------------------------------------------------------
-spec get_storage_type(storage:id()) -> helpers:name().
get_storage_type(StorageId) ->
    {ok, Doc} = storage:get(StorageId),
    {ok, #helper_init{name = StorageType}} = fslogic_storage:select_helper(Doc),
    StorageType.


%%--------------------------------------------------------------------
%% @doc Returns StorageId for given SpaceUUID
%% @end
%%--------------------------------------------------------------------
-spec get_storage_id(SpaceUUID :: file_meta:uuid()) -> storage:id().
get_storage_id(SpaceUUID) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} =
        space_storage:get(SpaceId),
    StorageId.


%%--------------------------------------------------------------------
%% @doc Generates storage GID based on SpaceName or SpaceUUID
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_gid(SpaceName :: file_meta:name(),
    SpaceUUID :: file_meta:uuid()) -> non_neg_integer().
gen_storage_gid(SpaceName, SpaceUUID) ->
    case helpers_nif:groupname_to_gid(SpaceName) of
        {ok, GID} ->
            GID;
        {error, _} ->
            fslogic_utils:gen_storage_uid(SpaceUUID)
    end.


%%--------------------------------------------------------------------
%% @doc Generates storage UID/GID based arbitrary binary (e.g. user's global id, space id, etc)
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_uid(ID :: binary()) -> non_neg_integer().
gen_storage_uid(?ROOT_USER_ID) ->
    0;
gen_storage_uid(ID) ->
    <<UID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, ID),
    {ok, LowestUID} = application:get_env(?APP_NAME,
        lowest_generated_storage_uid),
    {ok, HighestUID} = application:get_env(?APP_NAME,
        highest_generated_storage_uid),
    LowestUID + UID0 rem HighestUID.


%%--------------------------------------------------------------------
%% @doc Create random sequence consisting of lowercase ASCII letters.
%%--------------------------------------------------------------------
-spec random_ascii_lowercase_sequence(Length :: integer()) -> list().
random_ascii_lowercase_sequence(Length) ->
    lists:foldl(fun(_, Acc) -> [random:uniform(26) + 96 | Acc] end, [], lists:seq(1, Length)).


%%--------------------------------------------------------------------
%% @doc Returns parent of given file.
%%--------------------------------------------------------------------
-spec get_parent(fslogic_worker:file()) -> fslogic_worker:file() | no_return().
get_parent({path, Path}) ->
    [_ | R] = lists:reverse(fslogic_path:split(Path)),
    Tokens = lists:reverse(R),
    ParentPath = filepath_utils:ensure_begins_with_prefix(fslogic_path:join(Tokens), ?DIRECTORY_SEPARATOR_BINARY),
    {ok, Doc} = file_meta:get({path, ParentPath}),
    Doc;
get_parent(File) ->
    {ok, Doc} = file_meta:get_parent(File),
    Doc.

-spec gen_storage_file_id(Entry :: fslogic_worker:file()) ->
    helpers:file() | no_return().
gen_storage_file_id(Entry) ->
    {ok, Path} = file_meta:gen_storage_path(Entry),
    {ok, #document{value = #file_meta{version = Version}}} = file_meta:get(Entry),
    file_meta:snapshot_name(Path, Version).


-spec get_local_file_location(fslogic_worker:file()) ->
    datastore:document() | no_return().
get_local_file_location(Entry) -> %todo get rid of single file location and use get_local_file_locations/1
    [LocalLocation] = get_local_file_locations(Entry),
    LocalLocation.


-spec get_local_file_locations(fslogic_worker:file()) ->
    [datastore:document()] | no_return().
get_local_file_locations(Entry) ->
    LProviderId = oneprovider:get_provider_id(),
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    [Location ||
        {ok, Location = #document{value = #file_location{provider_id = ProviderId}}}
            <- Locations, LProviderId =:= ProviderId
    ].


-spec get_local_storage_file_locations(datastore:document() | #file_location{} | fslogic_worker:file()) ->
    [{storage:id(), helpers:file()}] | no_return().
get_local_storage_file_locations(#document{value = #file_location{} = Location}) ->
    get_local_storage_file_locations(Location);
get_local_storage_file_locations(#file_location{blocks = Blocks, storage_id = DSID, file_id = DFID}) ->
    lists:usort([{DSID, DFID} | [{SID, FID} || #file_block{storage_id = SID, file_id = FID} <- Blocks]]);
get_local_storage_file_locations(Entry) ->
    #document{} = Doc = get_local_file_location(Entry),
    get_local_storage_file_locations(Doc).


%%--------------------------------------------------------------------
%% @doc
%% Waiting for links document associated with file_meta to be present.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_links(file_meta:uuid(), non_neg_integer()) -> ok | no_return().
wait_for_links(FileUuid, 0) ->
    ?error("Waiting for links document, for file ~p failed.", [FileUuid]),
    throw(no_link_document);
wait_for_links(FileUuid, Retries) ->
    case file_meta:exists({uuid, links_utils:links_doc_key(FileUuid)}) of
        true ->
            ok;
        false ->
            timer:sleep(timer:seconds(1)),
            wait_for_links(FileUuid, Retries - 1)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Waiting for file_meta with given file_uuid to be present.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_file_meta(file_meta:uuid(), non_neg_integer()) -> ok | no_return().
wait_for_file_meta(FileUuid, 0) ->
    ?error("Waiting for file_meta ~p failed.", [FileUuid]),
    throw(no_file_meta_document);
wait_for_file_meta(FileUuid, Retries) ->
    case file_meta:exists({uuid, FileUuid}) of
        true ->
            ok;
        false ->
            timer:sleep(timer:seconds(1)),
            wait_for_links(FileUuid, Retries - 1)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets S3 credentials from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_s3_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, s3_user:credentials()} | undefined.
get_s3_user(UserId, StorageId) ->
    case s3_user:get(UserId) of
        {ok, #document{value = #s3_user{credentials = CredentialsMap}}} ->
            case maps:find(StorageId, CredentialsMap) of
                {ok, Credentials} ->
                    {ok, Credentials};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets Ceph credentials from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_ceph_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, ceph_user:credentials()} | undefined.
get_ceph_user(UserId, StorageId) ->
    case ceph_user:get(UserId) of
        {ok, #document{value = #ceph_user{credentials = CredentialsMap}}} ->
            case maps:find(StorageId, CredentialsMap) of
                {ok, Credentials} ->
                    {ok, Credentials};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets POSIX credentials from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user(UserId :: binary(), StorageId :: storage:id()) ->
    {ok, posix_user:credentials()} | undefined.
get_posix_user(UserId, StorageId) ->
    case posix_user:get(UserId) of
        {ok, #document{value = #posix_user{credentials = CredentialsMap}}} ->
            case maps:find(StorageId, CredentialsMap) of
                {ok, Credentials} ->
                    {ok, Credentials};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.