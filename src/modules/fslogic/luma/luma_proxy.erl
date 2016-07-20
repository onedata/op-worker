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
-include_lib("ctool/include/utils/utils.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").

%% API
-export([new_user_ctx/3, get_posix_user_ctx/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Retrieves user context from LUMA server.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec new_user_ctx(HelperInit :: helpers:init(), SessionId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> helpers_user:ctx().
new_user_ctx(#helper_init{name = ?CEPH_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_fetch_user_ctx(ceph_user, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?DIRECTIO_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_fetch_user_ctx(posix_user, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?S3_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_fetch_user_ctx(s3_user, SessionId, SpaceUUID);
new_user_ctx(#helper_init{name = ?SWIFT_HELPER_NAME}, SessionId, SpaceUUID) ->
    get_or_fetch_user_ctx(swift_user, SessionId, SpaceUUID).


%%--------------------------------------------------------------------
%% @doc Retrieves posix user ctx for file attrs from LUMA server.
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(StorageType :: helpers:name(), SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) -> #posix_user_ctx{}.
get_posix_user_ctx(?DIRECTIO_HELPER_NAME, SessionIdOrIdentity, SpaceUUID) ->
    get_or_fetch_user_ctx(posix_user, SessionIdOrIdentity, SpaceUUID);
get_posix_user_ctx(_, SessionIdOrIdentity, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionIdOrIdentity),
    StorageId = luma_utils:get_storage_id(SpaceUUID),

    case posix_user:get_ctx(UserId, StorageId) of
        #posix_user_ctx{} = Ctx -> Ctx;
        undefined ->
            {ok, Response} = get_credentials_from_luma(UserId, ?DIRECTIO_HELPER_NAME,
                undefined, SessionIdOrIdentity, SpaceUUID),
            UserCtx = make_user_ctx(posix_user, Response, SpaceUUID),
            posix_user:add(UserId, StorageId, UserCtx),
            UserCtx
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private @doc Retrieves user context from LUMA server for given storage helper.
%% This context may and should be used with helpers:set_user_ctx/2.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch_user_ctx(UserModel :: helpers_user:model(), SessId :: session:id(),
    SpaceUUID :: file_meta:uuid()) -> UserCtx :: helpers_user:ctx().
get_or_fetch_user_ctx(UserModel, SessionId, SpaceUUID) ->
    UserId = luma_utils:get_user_id(SessionId),
    StorageId = luma_utils:get_storage_id(SpaceUUID),

    case UserModel:get_ctx(UserId, StorageId) of
        undefined ->
            StorageType = luma_utils:get_storage_type(StorageId),
            {ok, Response} = get_credentials_from_luma(UserId, StorageType,
                StorageId, SessionId, SpaceUUID),
            UserCtx = make_user_ctx(UserModel, Response, SpaceUUID),
            UserModel:add(UserId, StorageId, UserCtx),
            UserCtx;
        UserCtx -> UserCtx
    end.

%%--------------------------------------------------------------------
%% @private @doc Creates user context from LUMA server response.
%%--------------------------------------------------------------------
-spec make_user_ctx(UserModel :: helpers_user:model(), Response :: proplists:proplist(),
    SpaceUUID :: file_meta:uuid()) -> UserCtx :: helpers_user:ctx().
make_user_ctx(ceph_user, Response, _SpaceUUID) ->
    UserName = proplists:get_value(<<"user_name">>, Response),
    UserKey = proplists:get_value(<<"user_key">>, Response),
    ceph_user:new_ctx(UserName, UserKey);
make_user_ctx(s3_user, Response, _SpaceUUID) ->
    AccessKey = proplists:get_value(<<"access_key">>, Response),
    SecretKey = proplists:get_value(<<"secret_key">>, Response),
    s3_user:new_ctx(AccessKey, SecretKey);
make_user_ctx(swift_user, Response, _SpaceUUID) ->
    UserName = proplists:get_value(<<"user_name">>, Response),
    Password = proplists:get_value(<<"password">>, Response),
    swift_user:new_ctx(UserName, Password);
make_user_ctx(posix_user, Response, SpaceUUID) ->
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
%% @private @doc Retrieves user credentials to storage from LUMA server.
%%--------------------------------------------------------------------
-spec get_credentials_from_luma(UserId :: binary(), StorageType :: helpers:name(),
    StorageId :: storage:id() | undefined, SessionIdOrIdentity :: session:id() | session:identity(),
    SpaceUUID :: file_meta:uuid()) ->
    {ok, proplists:proplist()} | {error, binary()}.
get_credentials_from_luma(UserId, StorageType, StorageId, SessionIdOrIdentity, SpaceUUID) ->
    {ok, LUMAHostname} = application:get_env(?APP_NAME, luma_hostname),
    {ok, LUMAPort} = application:get_env(?APP_NAME, luma_port),
    {ok, Hostname} = inet:gethostname(),
    {ok, {hostent, FullHostname, _, inet, _, IPList}} = inet:gethostbyname(Hostname),
    {ok, #document{value = #file_meta{name = SpaceName}}} = file_meta:get({uuid, SpaceUUID}),

    IPListParsed = lists:map(fun(IP) ->
        list_to_binary(inet_parse:ntoa(IP)) end, IPList),

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
        {ok, Auth} ->
            {ok, UserDetails} = oz_users:get_details(Auth),
            UserDetailsList = ?record_to_list(user_details, UserDetails),
            json_utils:encode(UserDetailsList)
    end,

    StorageIdParam = case StorageId of
        undefined ->
            <<"">>;
        _ ->
            <<"&storage_id=", StorageId/binary>>
    end,

    case http_client:get(
        <<(atom_to_binary(LUMAHostname, latin1))/binary, ":",
            (integer_to_binary(LUMAPort))/binary,
            "/get_user_credentials?"
            "global_id=", UserId/binary,
            "&storage_type=", StorageType/binary,
            StorageIdParam/binary,
            "&source_ips=", (json_utils:encode(IPListParsed))/binary,
            "&source_hostname=", (list_to_binary(FullHostname))/binary,
            "&space_name=", SpaceName/binary,
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
%% @private @doc Get auth from session ID, returns undefined when identity is given.
%%--------------------------------------------------------------------
-spec get_auth(session:id() | session:identity()) ->
    {ok, session:auth() | undefined} | {error, term()}.
get_auth(#identity{}) ->
    {ok, undefined};
get_auth(SessionId) ->
    session:get_auth(SessionId).