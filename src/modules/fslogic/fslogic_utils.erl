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

%% API
-export([random_ascii_lowercase_sequence/1, gen_storage_uid/1, get_parent/1, gen_storage_file_id/1]).
-export([get_local_file_location/1, get_local_storage_file_locations/1, get_credentials_from_luma/1]).
-export([get_storage_type/1, get_storage_id/1, get_posix_user_ctx/3]).


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
-spec get_storage_id(SpaceUUID :: binary()) -> storage:id().
get_storage_id(SpaceUUID) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    {ok, #document{value = #space_storage{storage_ids = [StorageId | _]}}} = space_storage:get(SpaceId),
    StorageId.


%%--------------------------------------------------------------------
%% @doc Retrieves user credentials to storage from LUMA
%% @end
%%--------------------------------------------------------------------
-spec get_credentials_from_luma(binary()) -> proplists:proplist().
get_credentials_from_luma(Params) ->
    case http_client:get(
        <<"172.19.160.194:5000/get_user_credentials?",Params/binary>>,
        [],
        [],
        [insecure]
    ) of
        {ok, 200, _Headers, Body} ->
            Json = json_utils:decode(Body),
            Status = proplists:get_value(<<"status">>, Json),
            case Status of
                <<"success">> ->
                    {ok, proplists:get_value(<<"data">>, Json)};
                <<"error">> ->
                    {error, proplists:get_value(<<"message">>, Json)}
            end;
        Error ->
            Error
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
    {ok, LowestUID} = application:get_env(?APP_NAME, lowest_generated_storage_uid),
    {ok, HighestUID} = application:get_env(?APP_NAME, highest_generated_storage_uid),
    LowestUID + UID0 rem HighestUID.


%%--------------------------------------------------------------------
%% @doc Retrieves posix user context from LUMA
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(onedata_user:id(), storage:id(), helpers:name()) -> #posix_user_ctx{}.
get_posix_user_ctx(?ROOT_USER_ID, _, _) ->
    ?ROOT_POSIX_CTX;
get_posix_user_ctx(UserId, StorageId, StorageType = ?DIRECTIO_HELPER_NAME) ->
    {ok, Response} = get_credentials_from_luma(<<"global_id=",UserId/binary,"&storage_id=",StorageId/binary,
        "&storage_type=",StorageType/binary>>),
    #posix_user_ctx{uid = proplists:get_value(<<"uid">>, Response), gid = proplists:get_value(<<"gid">>, Response)};
get_posix_user_ctx(UserId, _, _) ->
    {ok, Response} = get_credentials_from_luma(<<"global_id=",UserId/binary,
        "&storage_type=",?DIRECTIO_HELPER_NAME/binary>>),
    #posix_user_ctx{uid = proplists:get_value(<<"uid">>, Response), gid = proplists:get_value(<<"gid">>, Response)}.


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
get_local_file_location(Entry) ->
    LProviderId = oneprovider:get_provider_id(),
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    [LocalLocation] = [Location ||
        {ok, #document{value = #file_location{provider_id = ProviderId}} = Location}
            <- Locations, LProviderId =:= ProviderId
    ],
    LocalLocation.

-spec get_local_storage_file_locations(datastore:document() | #file_location{} | fslogic_worker:file()) ->
    [{storage:id(), helpers:file()}] | no_return().
get_local_storage_file_locations(#document{value = #file_location{} = Location}) ->
    get_local_storage_file_locations(Location);
get_local_storage_file_locations(#file_location{blocks = Blocks, storage_id = DSID, file_id = DFID}) ->
    lists:usort([{DSID, DFID} | [{SID, FID} || #file_block{storage_id = SID, file_id = FID} <- Blocks]]);
get_local_storage_file_locations(Entry) ->
    #document{} = Doc = get_local_file_location(Entry),
    get_local_storage_file_locations(Doc).
