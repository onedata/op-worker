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
-export([get_local_file_location/1, get_local_storage_file_locations/1, get_credentials_from_luma/3]).
-export([get_storage_type/1, get_storage_id/1, gen_storage_gid/2, parse_posix_ctx_from_luma/2]).


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
-spec get_credentials_from_luma(UserId :: binary(), StorageType :: helpers:name(),
    StorageId :: storage:id() | helpers:name()) -> proplists:proplist().
get_credentials_from_luma(UserId, StorageType, StorageId) ->
    {ok, LUMA_hostname} = application:get_env(?APP_NAME, luma_hostname),
    Hostname_binary = list_to_binary(LUMA_hostname),
    {ok, LUMA_port} = application:get_env(?APP_NAME, luma_port),
    Port_binary = list_to_binary(LUMA_port),
    {ok, Hostname} = inet:gethostname(),
    {ok, {hostent, Full_hostname, _, inet, _, [IP]}} = inet:gethostbyname(Hostname),
    Full_hostname_binary = list_to_binary(Full_hostname),
    IP_string = inet_parse:ntoa(IP),
    IP_binary = list_to_binary(IP_string),
    case http_client:get(
        <<Hostname_binary/binary,":",Port_binary/binary,"/get_user_credentials?global_id=",UserId/binary,
            "&storage_type=",StorageType/binary,"&storage_id=",StorageId/binary,"&source_ip=",IP_binary/binary,
            "&source_hostname=",Full_hostname_binary/binary>>,
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
%% @doc Parses LUMA response to posix user ctx
%% @end
%%--------------------------------------------------------------------
-spec parse_posix_ctx_from_luma(proplists:proplist(), SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
parse_posix_ctx_from_luma(Response, SpaceUUID) ->
    GID = case proplists:get_value(<<"gid">>, Response) of
              undefined ->
                  {ok, #document{value = #file_meta{name = SpaceName}}} = file_meta:get({uuid, SpaceUUID}),
                  gen_storage_gid(SpaceName, SpaceUUID);
              Val ->
                  Val
          end,
    #posix_user_ctx{uid = proplists:get_value(<<"uid">>, Response), gid = GID}.


%%--------------------------------------------------------------------
%% @doc Generates storage GID based on SpaceName or SpaceUUID
%% @end
%%--------------------------------------------------------------------
-spec gen_storage_gid(SpaceName :: file_meta:name(), SpaceUUID :: file_meta:uuid()) -> non_neg_integer().
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
    {ok, LowestUID} = application:get_env(?APP_NAME, lowest_generated_storage_uid),
    {ok, HighestUID} = application:get_env(?APP_NAME, highest_generated_storage_uid),
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
