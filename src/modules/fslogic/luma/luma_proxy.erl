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
-include("deps/ctool/include/utils/utils.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").

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
-spec new_user_ctx(StorageType :: helpers:init(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
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
-spec get_posix_user_ctx(StorageType :: helpers:name(), SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
get_posix_user_ctx(?DIRECTIO_HELPER_NAME, SessionIdOrIdentity, SpaceUUID) ->
    new_posix_user_ctx(SessionIdOrIdentity, SpaceUUID);
get_posix_user_ctx(_, SessionIdOrIdentity, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionIdOrIdentity),
    StorageId = luma_utils:get_storage_id(SpaceUUID),

    case luma_utils:get_posix_user(UserId, StorageId) of
        {ok, Credentials} ->
            #posix_user_ctx{
                uid = posix_user:uid(Credentials),
                gid = posix_user:gid(Credentials)
            };
        _ ->
            {ok, Response} = get_credentials_from_luma(UserId,
                ?DIRECTIO_HELPER_NAME, ?DIRECTIO_HELPER_NAME, SessionIdOrIdentity),

            UserCtx = parse_posix_ctx_from_luma(Response, SpaceUUID),
            posix_user:add(UserId, StorageId, UserCtx#posix_user_ctx.uid,
                UserCtx#posix_user_ctx.gid),
            UserCtx
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
-spec new_ceph_user_ctx(SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_ceph_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    StorageId = luma_utils:get_storage_id(SpaceUUID),

    case luma_utils:get_ceph_user(UserId, StorageId) of
        {ok, Credentials} ->
            #ceph_user_ctx{
                user_name = ceph_user:name(Credentials),
                user_key = ceph_user:key(Credentials)
            };
        undefined ->
            StorageType = luma_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType,
                StorageId, SessionId),

            UserName = proplists:get_value(<<"user_name">>, Response),
            UserKey = proplists:get_value(<<"user_key">>, Response),
            ceph_user:add(UserId, StorageId, UserName, UserKey),
            #ceph_user_ctx{user_name = UserName, user_key = UserKey}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user context from LUMA server for Amazon S3 storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_s3_user_ctx(SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_s3_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    StorageId = luma_utils:get_storage_id(SpaceUUID),
    case luma_utils:get_s3_user(UserId, StorageId) of
        {ok, Credentials} ->
            #s3_user_ctx{
                access_key = s3_user:access_key(Credentials),
                secret_key = s3_user:secret_key(Credentials)
            };
        _ ->
            StorageType = luma_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType,
                StorageId, SessionId),

            AccessKey = proplists:get_value(<<"access_key">>, Response),
            SecretKey = proplists:get_value(<<"secret_key">>, Response),
            s3_user:add(UserId, StorageId, AccessKey, SecretKey),

            #s3_user_ctx{
                access_key = AccessKey,
                secret_key = SecretKey
            }
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user context from LUMA server for all posix-compilant helpers
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_posix_user_ctx(SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_posix_user_ctx(SessionIdOrIdentity, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionIdOrIdentity),
    StorageId = luma_utils:get_storage_id(SpaceUUID),

    case luma_utils:get_posix_user(UserId, StorageId) of
        {ok, Credentials} ->
            #posix_user_ctx{
                uid = posix_user:uid(Credentials),
                gid = posix_user:gid(Credentials)
            };
        _ ->
            StorageType = luma_utils:get_storage_type(StorageId),

            {ok, Response} = get_credentials_from_luma(UserId, StorageType,
                StorageId, SessionIdOrIdentity),

            UserCtx = parse_posix_ctx_from_luma(Response, SpaceUUID),
            posix_user:add(UserId, StorageId, UserCtx#posix_user_ctx.uid,
                UserCtx#posix_user_ctx.gid),
            UserCtx
    end.


%%--------------------------------------------------------------------
%% @doc Retrieves user credentials to storage from LUMA
%% @end
%%--------------------------------------------------------------------
-spec get_credentials_from_luma(UserId :: binary(), StorageType :: helpers:name(),
    StorageId :: storage:id() | helpers:name(), SessionIdOrIdentity :: session:id() | session:identity()) ->
    {ok, proplists:proplist()} | {error, binary()}.
get_credentials_from_luma(UserId, StorageType, StorageId, SessionIdOrIdentity) ->
    {ok, LUMAHostname} = application:get_env(?APP_NAME, luma_hostname),
    {ok, LUMAPort} = application:get_env(?APP_NAME, luma_port),
    {ok, Hostname} = inet:gethostname(),
    {ok, {hostent, FullHostname, _, inet, _, IPList}} = inet:gethostbyname(Hostname),

    IPListParsed = lists:map(fun(IP) -> list_to_binary(inet_parse:ntoa(IP)) end, IPList),

    UserDetailsJSON = case get_auth(SessionIdOrIdentity) of
        {ok, undefined} ->
            UserId = luma_utils:get_user_id(SessionIdOrIdentity),
            case onedata_user:get(UserId) of
                {ok, #document{value = #onedata_user{name = Name, alias = Alias,
                    email_list = EmailList, connected_accounts = ConnectedAccounts}}} ->

                    UserDetailsList = ?record_to_list(user_details, #user_details{
                        name = Name,
                        alias = Alias,
                        connected_accounts = ConnectedAccounts,
                        email_list = EmailList,
                        id = UserId
                    }),
                    json_utils:encode(UserDetailsList);
                {error, {not_found, onedata_user}} ->
                    <<"{}">>
            end;
        {ok, #auth{macaroon = Macaroon, disch_macaroons = DMacaroons}} ->
            {ok, UserDetails} = oz_users:get_details({user,
                {Macaroon, DMacaroons}}),
            UserDetailsList = ?record_to_list(user_details, UserDetails),
            json_utils:encode(UserDetailsList)
    end,

    case http_client:get(
        <<(atom_to_binary(LUMAHostname, latin1))/binary, ":",
            (integer_to_binary(LUMAPort))/binary,
            "/get_user_credentials?"
            "global_id=", UserId/binary,
            "&storage_type=", StorageType/binary,
            "&storage_id=", StorageId/binary,
            "&source_ips=", (json_utils:encode(IPListParsed))/binary,
            "&source_hostname=", (list_to_binary(FullHostname))/binary,
            "&user_details=", (http_utils:url_encode(UserDetailsJSON))/binary>>,
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
-spec parse_posix_ctx_from_luma(proplists:proplist(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
parse_posix_ctx_from_luma(Response, SpaceUUID) ->
    GID = case proplists:get_value(<<"gid">>, Response) of
        undefined ->
            {ok, #document{value = #file_meta{name = SpaceName}}} =
                file_meta:get({uuid, SpaceUUID}),
            luma_utils:gen_storage_gid(SpaceName, SpaceUUID);
        Val ->
            Val
    end,
    #posix_user_ctx{uid = proplists:get_value(<<"uid">>, Response), gid = GID}.


%%--------------------------------------------------------------------
%% @doc
%% Get auth from session_id, returns undefined when identity is given.
%% @end
%%--------------------------------------------------------------------
-spec get_auth(session:id() | session:identity()) ->
    {ok, session:auth() | undefined} | {error, term()}.
get_auth(#identity{}) ->
    {ok, undefined};
get_auth(SessionId) ->
    session:get_auth(SessionId).
