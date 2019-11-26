%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for querying external, third party LUMA service
%%% for storage user context.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_proxy).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([get_user_ctx/5, get_request_headers/1, get_group_ctx/4]).

%% exported for test reasons
-export([http_client_post/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Queries third party LUMA service for the storage user context.
%% @end
%%--------------------------------------------------------------------
-spec get_user_ctx(session:id(), od_user:id(), od_space:id(), storage:doc(), storage:helper()) ->
    {ok, luma:user_ctx()} | {error, Reason :: term()}.
get_user_ctx(SessionId, UserId, SpaceId, StorageDoc = #document{
    value = #storage{
        luma_config = LumaConfig = #luma_config{url = LumaUrl}
}}, Helper) ->
    Url = str_utils:format_bin("~s/map_user_credentials", [LumaUrl]),
    ReqHeaders = get_request_headers(LumaConfig),
    ReqBody = get_request_body(SessionId, UserId, SpaceId, StorageDoc),
    case luma_proxy:http_client_post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            UserCtx = json_utils:decode(RespBody),
            UserCtxBinaries = integers_to_binary(UserCtx),
            HelperName = helper:get_name(Helper),
            case helper_params:validate_user_ctx(HelperName, UserCtxBinaries) of
                ok -> {ok, UserCtxBinaries};
                {error, Reason} -> {error, Reason}
            end;
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Queries third party LUMA service for the storage GID for given GroupId.
%% @end
%%-------------------------------------------------------------------
-spec get_group_ctx(od_group:id() | undefined, od_space:id(), storage:doc(), storage:helper()) ->
    {ok, luma:group_ctx()} | {error, term()}.
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?CEPH_HELPER_NAME}) ->
    undefined;
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?CEPHRADOS_HELPER_NAME}) ->
    undefined;
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?S3_HELPER_NAME}) ->
    undefined;
get_group_ctx(_GroupId, _SpaceId, _StorageDoc, #helper{name = ?SWIFT_HELPER_NAME}) ->
    undefined;
get_group_ctx(GroupId, SpaceId, StorageDoc = #document{
    value = #storage{
        luma_config = LumaConfig = #luma_config{url = LumaUrl}
}}, Helper) ->
    Url = str_utils:format_bin("~s/map_group", [LumaUrl]),
    ReqHeaders = get_request_headers(LumaConfig),
    ReqBody = get_group_request_body(GroupId, SpaceId, StorageDoc),
    case luma_proxy:http_client_post(Url, ReqHeaders, ReqBody) of
        {ok, 200, _RespHeaders, RespBody} ->
            GroupCtx = json_utils:decode(RespBody),
            GroupCtxBinaries = integers_to_binary(GroupCtx),
            case helper_params:validate_group_ctx(Helper, GroupCtxBinaries) of
                ok ->
                    {ok, GroupCtxBinaries};
                Error = {error, Reason} ->
                    ?error_stacktrace("Invalid group ctx returned from map_group request: ~p", [Reason]),
                    Error
            end;
        {ok, Code, _RespHeaders, RespBody} ->
            {error, {Code, json_utils:decode(RespBody)}};
        {error, Reason} ->
            {error, Reason}
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns LUMA request headers based on #luma_config.
%% @end
%%-------------------------------------------------------------------
-spec get_request_headers(luma_config:config()) -> map().
get_request_headers(#luma_config{api_key = undefined}) ->
    #{?HDR_CONTENT_TYPE => <<"application/json">>};
get_request_headers(#luma_config{api_key = APIKey}) ->
    #{
        ?HDR_CONTENT_TYPE => <<"application/json">>,
        ?HDR_X_AUTH_TOKEN => APIKey
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Simple wrapper for http_client:post.
%% This function is used to avoid mocking http_client in tests.
%% Mocking http_client is dangerous because meck's reloading and
%% purging the module can result in node_manager being killed.
%%-------------------------------------------------------------------
-spec http_client_post(http_client:url(), http_client:headers(),
    http_client:body()) -> http_client:response().
http_client_post(Url, ReqHeaders, ReqBody) ->
    http_client:post(Url, ReqHeaders, ReqBody).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external LUMA service.
%% @end
%%--------------------------------------------------------------------
-spec get_request_body(session:id(), od_user:id(), od_space:id(), storage:doc()) ->
    Body :: binary().
get_request_body(SessionId, UserId, SpaceId, StorageDoc) ->
    Body = #{
        <<"storageId">> => storage:get_id(StorageDoc),
        <<"storageName">> => storage:get_name(StorageDoc),
        <<"spaceId">> => SpaceId,
        <<"userDetails">> => get_user_details(SessionId, UserId)
    },
    json_utils:encode(filter_null_and_undefined_values(Body)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user context request that will be sent to the external LUMA service.
%% @end
%%--------------------------------------------------------------------
-spec get_group_request_body(od_group:id() | undefined, od_space:id(), storage:doc()) ->
    Body :: binary().
get_group_request_body(undefined, SpaceId, #document{
    key = StorageId,
    value = #storage{name = StorageName}
}) ->
    Body = #{
        <<"spaceId">> => SpaceId,
        <<"storageId">> => StorageId,
        <<"storageName">> => StorageName
    },
    json_utils:encode(filter_null_and_undefined_values(Body));
get_group_request_body(GroupId, SpaceId, #document{
    key = StorageId,
    value = #storage{name = StorageName}
}) ->
    Body = #{
        <<"groupId">> => GroupId,
        <<"spaceId">> => SpaceId,
        <<"storageId">> => StorageId,
        <<"storageName">> => StorageName
    },
    json_utils:encode(filter_null_and_undefined_values(Body)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs user details map.
%% @end
%%--------------------------------------------------------------------
-spec get_user_details(session:id(), od_user:id()) -> UserDetails :: map().
get_user_details(SessionId, UserId) ->
    case user_logic:get_protected_data(SessionId, UserId) of
        {ok, #document{value = User}} ->
            #{
                <<"id">> => UserId,
                <<"username">> => User#od_user.username,
                <<"emails">> => User#od_user.emails,
                <<"linkedAccounts">> => User#od_user.linked_accounts,

                %% @TODO VFS-4506 deprecated fields, included for backward compatibility
                %% @TODO VFS-4506 fullName and linkedAccounts.entitlements are no longer
                %% sent to LUMA as they are ambiguous and inconclusive for user mapping
                <<"fullName">> => <<>>,
                <<"name">> => <<>>,
                <<"login">> => User#od_user.username,
                <<"emailList">> => User#od_user.emails
            };
        {error, _} ->
            #{
                <<"id">> => UserId,
                <<"username">> => <<>>,
                <<"emails">> => [],
                <<"linkedAccounts">> => [],

                %% @TODO VFS-4506 deprecated fields, included for backward compatibility
                %% @TODO VFS-4506 fullName and linkedAccounts.entitlements are no longer
                %% sent to LUMA as they are ambiguous and inconclusive for user mapping
                <<"fullName">> => <<>>,
                <<"name">> => <<>>,
                <<"login">> => <<>>,
                <<"emailList">> => []
            }
    end.

-spec filter_null_and_undefined_values(term()) -> term().
filter_null_and_undefined_values(Map) when is_map(Map) ->
    maps:fold(fun(Key, Value, AccIn) ->
        FilteredValue = filter_null_and_undefined_values(Value),
        case should_filter(FilteredValue) of
            true ->
                AccIn;
            false ->
                AccIn#{Key => FilteredValue}
        end
    end, #{}, Map);
filter_null_and_undefined_values(List) when is_list(List) ->
    lists:filtermap(fun(Element) ->
        FilteredElement = filter_null_and_undefined_values(Element),
        case should_filter(FilteredElement) of
            true ->
                false;
            false ->
                {true, FilteredElement}
        end
    end, List);
filter_null_and_undefined_values(OtherValue) ->
    OtherValue.


-spec should_filter(term()) -> boolean().
should_filter(null) ->
    true;
should_filter(<<"null">>) ->
    true;
should_filter(undefined) ->
    true;
should_filter(<<"undefined">>) ->
    true;
should_filter(Map) when is_map(Map) andalso map_size(Map) =:= 0 ->
    true;
should_filter(_) ->
    false.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Converts all integer values in a map to binary.
%% @end
%%-------------------------------------------------------------------
-spec integers_to_binary(map()) -> map().
integers_to_binary(Map) ->
    maps:map(fun
        (_, Value) when is_integer(Value) -> integer_to_binary(Value);
        (_, Value) -> Value
    end, Map).
