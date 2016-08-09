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
    new_s3_user_ctx(SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?SWIFT_HELPER_NAME}, SessionId, SpaceUUID) ->
    new_swift_user_ctx(SessionId, SpaceUUID).


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
            {ok, Response} = get_credentials_from_luma(UserId, ?DIRECTIO_HELPER_NAME,
                undefined, SessionIdOrIdentity, SpaceUUID),

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
                StorageId, SessionId, SpaceUUID),

            UserName = proplists:get_value(<<"userName">>, Response),
            UserKey = proplists:get_value(<<"userKey">>, Response),
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
                StorageId, SessionId, SpaceUUID),

            AccessKey = proplists:get_value(<<"accessKey">>, Response),
            SecretKey = proplists:get_value(<<"secretKey">>, Response),
            s3_user:add(UserId, StorageId, AccessKey, SecretKey),

            #s3_user_ctx{
                access_key = AccessKey,
                secret_key = SecretKey
            }
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates new user's storage context for Swift storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_swift_user_ctx(SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers:user_ctx().
new_swift_user_ctx(SessionId, SpaceUUID) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    StorageId = luma_utils:get_storage_id(SpaceUUID),
    case luma_utils:get_swift_user(UserId, StorageId) of
        {ok, Credentials} ->
            #swift_user_ctx{
                user_name = swift_user:user_name(Credentials),
                password = swift_user:password(Credentials)
            };
        _ ->
            StorageType = luma_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType,
                StorageId, SessionId, SpaceUUID),

            UserName = proplists:get_value(<<"userName">>, Response),
            Password = proplists:get_value(<<"password">>, Response),
            swift_user:add(UserId, StorageId, UserName, Password),

            #swift_user_ctx{
                user_name = UserName,
                password = Password
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
                StorageId, SessionIdOrIdentity, SpaceUUID),

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
    StorageId :: storage:id() | undefined, SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) ->
    {ok, proplists:proplist()} | {error, binary()}.
get_credentials_from_luma(UserId, StorageType, StorageId, SessionIdOrIdentity, SpaceUUID) ->
    {ok, Hostname} = application:get_env(?APP_NAME, luma_hostname),
    {ok, Port} = application:get_env(?APP_NAME, luma_port),
    {ok, Scheme} = application:get_env(?APP_NAME, luma_scheme),
    {ok, APIKey} = application:get_env(?APP_NAME, luma_api_key),
    {ok, #document{value = #file_meta{name = SpaceName}}} = file_meta:get({uuid, SpaceUUID}),

    UserDetails = case get_auth(SessionIdOrIdentity) of
        {ok, undefined} ->
            case onedata_user:get(UserId) of
                {ok, #document{value = #onedata_user{name = Name, alias = Alias,
                    email_list = EmailList, connected_accounts = ConnectedAccounts}}} ->

                    #user_details{
                        name = Name,
                        alias = Alias,
                        connected_accounts = ConnectedAccounts,
                        email_list = EmailList,
                        id = UserId
                    };
                {error, {not_found, onedata_user}} ->
                    #user_details{
                        name = <<"">>,
                        alias = <<"">>,
                        connected_accounts = [],
                        email_list = [],
                        id = UserId
                    }
            end;
        {ok, Auth} ->
            {ok, #document{value = #onedata_user{name = Name, alias = Alias,
                email_list = EmailList, connected_accounts = ConnectedAccounts}}} =
                onedata_user:get_or_fetch(Auth, UserId),
            #user_details{
                name = Name,
                alias = Alias,
                connected_accounts = ConnectedAccounts,
                email_list = EmailList,
                id = UserId
            }
    end,

    case http_client:post(
        <<(atom_to_binary(Scheme, latin1))/binary, "://",
            (atom_to_binary(Hostname, latin1))/binary,
            ":", (integer_to_binary(Port))/binary,
            "/map_user_credentials">>,
        [{<<"X-Auth-Token">>, atom_to_binary(APIKey, latin1)},
            {<<"Content-Type">>, <<"application/json">>}],
        encode_body(StorageType, StorageId, SpaceName, UserDetails)
    ) of
        {ok, 200, _Headers, Body} ->
            {ok, json_utils:decode(Body)};
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


%%--------------------------------------------------------------------
%% @doc
%% Encodes StorageType, StorageId, SpaceName and UserDetails to
%% JSON binary.
%% @end
%%--------------------------------------------------------------------
-spec encode_body(StorageType :: helpers:name(),
    StorageId :: storage:id() | undefined, SpaceName :: file_meta:name(),
    UserDetails :: #user_details{}) -> binary().
encode_body(StorageType, StorageId, SpaceName, UserDetails) ->
    {ok, NameToTypeMap} = application:get_env(?APP_NAME, helper_name_to_luma_storage_type_mapping),
    Body = [{<<"storageType">>, maps:get(StorageType, NameToTypeMap)},
        {<<"spaceName">>, SpaceName}],

    BodyWithStorageId = case StorageId of
        undefined -> Body;
        _ -> lists:append(Body, [{<<"storageId">>, StorageId}])
    end,

    #user_details{
        id = Id,
        name = Name,
        connected_accounts = ConnectedAccounts,
        alias = Alias,
        email_list = EmailList
    } = UserDetails,
    ParsedConnectedAccounts = lists:map(fun(Account) ->
        [
            {<<"providerId">>, proplists:get_value(<<"provider_id">>, Account)},
            {<<"userId">>, proplists:get_value(<<"user_id">>, Account)},
            {<<"login">>, proplists:get_value(<<"login">>, Account)},
            {<<"name">>, proplists:get_value(<<"name">>, Account)},
            {<<"emailList">>, proplists:get_value(<<"email_list">>, Account)}
        ]
    end, ConnectedAccounts),

    BodyWithUserDetails = lists:append(BodyWithStorageId, [{<<"userDetails">>, [
        {<<"id">>, Id},
        {<<"name">>, Name},
        {<<"connectedAccounts">>, ParsedConnectedAccounts},
        {<<"alias">>, Alias},
        {<<"emailList">>, EmailList}
    ]}]),

    json_utils:encode(BodyWithUserDetails).
