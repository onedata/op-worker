%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module for requesting user mapping from LUMA server
%%% @end
%%%-------------------------------------------------------------------
-module(luma_proxy).
-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").


%% API
-export([new_user_ctx/3, get_posix_user_ctx/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves user context from LUMA server.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_user_ctx(StorageType :: helpers:init(), SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) ->
    helpers:user_ctx().
new_user_ctx(#helper_init{name = ?CEPH_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_ceph_user_ctx(SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?DIRECTIO_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_posix_user_ctx(SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?S3_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_s3_user_ctx(SessionId, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc Retrieves posix user ctx for file attrs from LUMA server
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(StorageType :: helpers:name(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
get_posix_user_ctx(?DIRECTIO_HELPER_NAME, SessionId, SpaceUUID) ->
    new_posix_user_ctx(SessionId, SpaceUUID);
get_posix_user_ctx(_, SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    case luma_response:get(UserId, ?DIRECTIO_HELPER_NAME) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            {ok, Response} = get_credentials_from_luma(UserId, ?DIRECTIO_HELPER_NAME,
            ?DIRECTIO_HELPER_NAME, SessionId),
            User_ctx = parse_posix_ctx_from_luma(Response, SpaceUUID),
            luma_response:save(UserId, ?DIRECTIO_HELPER_NAME, User_ctx),
            User_ctx
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves user context from LUMA server for Ceph storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_ceph_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_ceph_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case luma_response:get(UserId, StorageId) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            StorageType = fslogic_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType, StorageId, SessionId),
            User_ctx = #ceph_user_ctx{
                user_name = proplists:get_value(<<"user_name">>, Response),
                user_key = proplists:get_value(<<"user_key">>, Response)
            },
            luma_response:save(UserId, StorageId, User_ctx),
            User_ctx
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user context from LUMA server for all posix-compilant helpers
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_posix_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case luma_response:get(UserId, StorageId) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            StorageType = fslogic_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType, StorageId, SessionId),
            User_ctx = parse_posix_ctx_from_luma(Response, SpaceUUID),
            luma_response:save(UserId, StorageId, User_ctx),
            User_ctx
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user context from LUMA server for Amazon S3 storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(SessionId :: session:id(), SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_s3_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    StorageId = fslogic_utils:get_storage_id(SpaceUUID),
    case luma_response:get(UserId, StorageId) of
        {ok, Credentials} ->
            Credentials;
        _ ->
            StorageType = fslogic_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType, StorageId, SessionId),
            User_ctx = #s3_user_ctx{
                access_key = proplists:get_value(<<"access_key">>, Response),
                secret_key = proplists:get_value(<<"secret_key">>, Response)
            },
            luma_response:save(UserId, StorageId, User_ctx),
            User_ctx
    end.


%%--------------------------------------------------------------------
%% @doc Retrieves user credentials to storage from LUMA
%% @end
%%--------------------------------------------------------------------
-spec get_credentials_from_luma(UserId :: binary(), StorageType :: helpers:name(),
    StorageId :: storage:id() | helpers:name(), SessionId :: session:id()) -> proplists:proplist().
get_credentials_from_luma(UserId, StorageType, StorageId, SessionId) ->
    {ok, LUMA_hostname} = application:get_env(?APP_NAME, luma_hostname),
    Hostname_binary = list_to_binary(LUMA_hostname),
    {ok, LUMA_port} = application:get_env(?APP_NAME, luma_port),
    Port_binary = list_to_binary(LUMA_port),
    {ok, Hostname} = inet:gethostname(),
    {ok, {hostent, Full_hostname, _, inet, _, IP_list}} = inet:gethostbyname(Hostname),
    Full_hostname_binary = list_to_binary(Full_hostname),
    IP_list_string = lists:map(fun(IP) -> list_to_binary(inet_parse:ntoa(IP)) end, IP_list),
    IP_binary = json_utils:encode(IP_list_string),
    User_details_json = case session:get_auth(SessionId) of
        {ok, undefined} ->
            <<"{}">>;
        {ok, #auth{macaroon = SrlzdMacaroon, disch_macaroons = SrlzdDMacaroons}} ->
            {ok, User_details} = gr_users:get_details({user, {SrlzdMacaroon, SrlzdDMacaroons}}),
            User_details_proplist = lists:zip(record_info(fields, user_details), tl(tuple_to_list(User_details))),
            list_to_binary(http_uri:encode(binary_to_list(json_utils:encode(User_details_proplist))))
    end,
    case http_client:get(
        <<Hostname_binary/binary,":",Port_binary/binary,"/get_user_credentials?global_id=",UserId/binary,
        "&storage_type=",StorageType/binary,"&storage_id=",StorageId/binary,"&source_ips=",IP_binary/binary,
        "&source_hostname=",Full_hostname_binary/binary,"&user_details=",User_details_json/binary>>,
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
            fslogic_utils:gen_storage_gid(SpaceName, SpaceUUID);
        Val ->
            Val
    end,
    #posix_user_ctx{uid = proplists:get_value(<<"uid">>, Response), gid = GID}.
