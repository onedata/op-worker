%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for mapping onedata users to storage users.
%%% @end
%%%-------------------------------------------------------------------
-module(luma).
-author("Krzysztof Trzepla").
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_server_user_ctx/4, get_server_user_ctx/5, get_client_user_ctx/4,
    get_posix_user_ctx/3, get_posix_user_ctx/4, add_helper_specific_fields/4]).

% exported for CT tests
-export([get_admin_ctx/2, get_insecure_user_ctx/1, generate_user_ctx/2,
    generate_group_ctx/3]).

-type user_ctx() :: helper:user_ctx().
-type group_ctx() :: helper:group_ctx().
-type uid() :: non_neg_integer().
-type gid() :: non_neg_integer().
-type posix_user_ctx() :: {uid(), gid()}.

-export_type([user_ctx/0, posix_user_ctx/0, gid/0, group_ctx/0, uid/0]).

-define(KEY_SEPARATOR, <<"::">>).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns storage user context associated with the chosen storage helper,
%% which is appropriate for the local server operations.
%% First, if user context has been requested on behalf of root user, storage
%% admin context are returned. Next external, third party LUMA service is
%% queried. Finally for POSIX storage user context is generated and for other
%% storages storage admin context is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_server_user_ctx(session:id(), od_user:id(), od_space:id(),
    od_storage:id() | storage:record()) -> {ok, user_ctx()} | {error, Reason :: term()}.
get_server_user_ctx(SessionId, UserId, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    get_server_user_ctx(SessionId, UserId, SpaceId, Storage);
get_server_user_ctx(SessionId, UserId, SpaceId, Storage) ->
    Helper = storage:get_helper(Storage),
    HelperName = helper:get_name(Helper),
    StorageId = storage:get_id(Storage),
    Result = luma_cache:get_user_ctx(UserId, StorageId, fun() ->
        get_user_ctx([
            {fun luma:get_admin_ctx/2, [UserId, Helper]},
            {fun fetch_user_ctx/5, [SessionId, UserId, SpaceId, Storage, Helper]},
            {fun maybe_generate_user_ctx/3, [UserId, SpaceId, HelperName]},
            {fun luma:get_insecure_user_ctx/1, [Helper]}
        ])
    end, HelperName),
    case Result of
        {ok, UserCtx} ->
            add_helper_specific_fields(UserId, SessionId, UserCtx, Helper);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage user context associated with the chosen storage helper,
%% which is appropriate for the remote client operations working in direct IO
%% mode. First an external, third party LUMA service is queried. Next, if the
%% storage helper is defined as insecure, storage admin context is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_client_user_ctx(session:id(), od_user:id(), od_space:id(),
    storage:record()) -> {ok, user_ctx()} | {error, Reason :: term()}.
get_client_user_ctx(SessionId, UserId, SpaceId, Storage) ->
    Helper = storage:get_helper(Storage),
    HelperName = helper:get_name(Helper),
    StorageId = storage:get_id(Storage),
    Result = luma_cache:get_user_ctx(UserId, StorageId, fun() ->
        get_user_ctx([
            {fun fetch_user_ctx/5, [SessionId, UserId, SpaceId, Storage, Helper]},
            {fun luma:get_insecure_user_ctx/1, [Helper]},
            {fun maybe_generate_user_ctx/3, [UserId, SpaceId, HelperName]}
        ])
    end, HelperName),
    case Result of
        {ok, UserCtx} ->
            add_helper_specific_fields(UserId, SessionId, UserCtx, Helper);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% For a space supported by a POSIX storage returns POSIX user context
%% (UID and GID), otherwise generates it.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(session:id(), od_user:id(), od_space:id()) ->
    posix_user_ctx().
get_posix_user_ctx(?ROOT_SESS_ID, ?ROOT_USER_ID, _SpaceId) ->
    {0, 0};
get_posix_user_ctx(SessionId, UserId, SpaceId) ->
    {ok, UserCtx} = case select_posix_compatible_storage(SpaceId) of
        {ok, Storage} ->
            luma:get_server_user_ctx(SessionId, UserId, SpaceId, Storage);
        {error, not_found} ->
            maybe_generate_user_ctx(UserId, SpaceId, ?POSIX_HELPER_NAME)
    end,
    #{<<"uid">> := Uid, <<"gid">> := Gid} = UserCtx,
    {binary_to_integer(Uid), binary_to_integer(Gid)}.

%%--------------------------------------------------------------------
%% @doc
%% For a space supported by a POSIX storage returns POSIX user context
%% (UID and GID), otherwise generates it.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(session:id(), od_user:id(), undefined | od_group:id(),
    od_space:id()) -> posix_user_ctx().
get_posix_user_ctx(SessionId, UserId, GroupId, SpaceId) ->
    {ok, UserCtx} = case select_posix_compatible_storage(SpaceId) of
        {ok, Storage} ->
            get_server_user_ctx(SessionId, UserId, GroupId, SpaceId, Storage);
        {error, not_found} ->
            maybe_generate_user_ctx(UserId, GroupId, SpaceId, ?POSIX_HELPER_NAME)
    end,
    #{<<"uid">> := Uid, <<"gid">> := Gid} = UserCtx,
    {binary_to_integer(Uid), binary_to_integer(Gid)}.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage user context associated with the chosen storage helper
%% which is appropriate for the local server operations and given GroupId,
%% First, if user context has been requested on behalf of root user, storage
%% admin context are returned. Next external, third party LUMA service is
%% queried. Finally for POSIX storage user context is generated and for other
%% storages storage admin context is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_server_user_ctx(session:id(), od_user:id(), undefined | od_group:id(),
    od_space:id(), storage:record() | od_storage:id()) ->
    {ok, user_ctx()} | {error, Reason :: term()}.
get_server_user_ctx(SessionId, UserId, GroupId, SpaceId, StorageId) when is_binary(StorageId)->
    {ok, Storage} = storage:get(StorageId),
    get_server_user_ctx(SessionId, UserId, GroupId, SpaceId, Storage);
get_server_user_ctx(SessionId, UserId, GroupId, SpaceId, Storage) ->
    Helper = storage:get_helper(Storage),
    HelperName = helper:get_name(Helper),
    StorageId = storage:get_id(Storage),
    Result = luma_cache:get_user_ctx(UserId, StorageId,
        utils:ensure_defined(GroupId, undefined, SpaceId), fun() ->
            get_user_ctx([
                {fun luma:get_admin_ctx/2, [UserId, Helper]},
                {fun fetch_user_ctx/6, [SessionId, UserId, GroupId,
                    SpaceId, Storage, Helper]},
                {fun maybe_generate_user_ctx/4, [UserId, GroupId,
                SpaceId, HelperName]},
                {fun luma:get_insecure_user_ctx/1, [Helper]}
            ])
        end, HelperName
    ),
    case Result of
        {ok, UserCtx} ->
            add_helper_specific_fields(UserId, SessionId, UserCtx, Helper);
        Error ->
            Error
    end.

%%%===================================================================
%%% Exported for CT tests
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% For the root user returns storage admin context, otherwise 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec get_admin_ctx(od_user:id(), helpers:helper()) -> {ok, user_ctx()} | undefined.
get_admin_ctx(?ROOT_USER_ID, #helper{admin_ctx = AdminCtx}) ->
    {ok, AdminCtx};
get_admin_ctx(_UserId, _Helper) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% For the insecure storage helper returns storage admin context, otherwise
%% 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec get_insecure_user_ctx(helpers:helper()) -> {ok, user_ctx()} | undefined.
get_insecure_user_ctx(#helper{name = HelperName})
    when HelperName =:= ?POSIX_HELPER_NAME
    orelse HelperName =:= ?GLUSTERFS_HELPER_NAME
    orelse HelperName =:= ?NULL_DEVICE_HELPER_NAME
    ->
    undefined;
get_insecure_user_ctx(#helper{insecure = true, admin_ctx = AdminCtx}) ->
    {ok, AdminCtx};
get_insecure_user_ctx(_Helper) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% Generates user context (UID and GID) as a hash of respectively user
%% ID and group/space ID. For the other storage returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec generate_user_ctx(od_user:id(), od_space:id() | od_group:id()) ->
    {ok, helper:user_ctx()}.
generate_user_ctx(?ROOT_USER_ID, _) ->
    {ok, #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>}};
generate_user_ctx(UserId, GroupOrSpaceId) ->
    {ok, UidRange} = application:get_env(?APP_NAME, luma_posix_uid_range),
    {ok, GidRange} = application:get_env(?APP_NAME, luma_posix_gid_range),
    Uid = generate_posix_identifier(UserId, UidRange),
    Gid = generate_posix_identifier(GroupOrSpaceId, GidRange),
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% For the POSIX storage generates group context (GID) as a hash of
%% group ID or space ID (if group ID is undefined).
%% For the other storage returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec generate_group_ctx(od_user:id(), od_group:id() | undefined, od_space:id()) ->
    {ok, group_ctx()} | undefined.
generate_group_ctx(?ROOT_USER_ID, _GroupId, _SpaceId) ->
    {ok, #{<<"gid">> => <<"0">>}};
generate_group_ctx(_UserId, undefined, SpaceId) ->
    generate_posix_group_ctx(SpaceId);
generate_group_ctx(_UserId, GroupId, _SpaceId) ->
    generate_posix_group_ctx(GroupId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns storage user context by evaluating provided strategies.
%% Evaluation is stopped when the first strategy yield concrete result,
%% i.e. different than 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec get_user_ctx([{function(), Args :: [term()]}]) ->
    {ok, user_ctx()} | {error, Reason :: term()}.
get_user_ctx(Strategies) ->
    Result = lists:foldl(fun
        ({Function, Args}, undefined) -> apply(Function, Args);
        (_Strategy, PrevResult) -> PrevResult
    end, undefined, Strategies),

    case Result of
        undefined -> {error, undefined_user_context};
        _ -> Result
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Queries external, third party LUMA service for the user context if enabled.
%% Fails with an error if the response is erroneous.
%% @end
%%--------------------------------------------------------------------
-spec fetch_user_ctx(session:id(), od_user:id(), od_space:id(), storage:record(),
    helpers:helper()) -> {ok, user_ctx()} | {error, Reason :: term()} | undefined.
fetch_user_ctx(SessionId, UserId, SpaceId, Storage, Helper) ->
    case storage:is_luma_enabled(Storage) of
        false ->
            undefined;
        true ->
            Result = luma_proxy:get_user_ctx(SessionId, UserId, SpaceId,
                Storage, Helper),
            case Result of
                {error, Reason} ->
                    {error, {luma_server, Reason}};
                Other ->
                    Other
            end
    end.

-spec add_helper_specific_fields(od_user:id(), session:id(), user_ctx(),
    helpers:helper()) -> {ok, user_ctx()} | {errro, term()}.
add_helper_specific_fields(UserId, SessionId, UserCtx = #{
    <<"credentialsType">> := <<"oauth2">>
}, Helper = #helper{name = ?WEBDAV_HELPER_NAME}) %currently oauth2 is supported only for WebDav
    ->
    fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper);
add_helper_specific_fields(_UserId, _SessionId, UserCtx, _Helper) ->
    {ok, UserCtx}.

-spec fill_in_webdav_oauth2_token(od_user:id(), session:id(), user_ctx(),
    helpers:helper()) -> {ok, user_ctx()} | {errro, term()}.
fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper = #helper{
    args = #{<<"oauth2IdP">> := OAuth2IdP}
}) ->
    fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper, OAuth2IdP);
fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper = #helper{}) ->
    % OAuth2IdP was not explicitly set, try to infer it
    case provider_logic:zone_get_offline_access_idps() of
        {ok, [OAuth2IdP]} ->
            fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, Helper, OAuth2IdP);
        {ok, []} ->
            ?error("Empty list of identity providers retrieved from Onezone"),
            {error, missing_identity_provider};
        {ok, _} ->
            ?error("Ambiguous list of identity providers retrieved from Onezone"),
            {error, ambiguous_identity_provider}
    end.

-spec fill_in_webdav_oauth2_token(od_user:id(), session:id(), user_ctx(),
    helpers:helper(), binary()) -> {ok, user_ctx()} | {errro, term()}.
fill_in_webdav_oauth2_token(?ROOT_USER_ID, ?ROOT_SESS_ID, AdminCtx = #{
    <<"onedataAccessToken">> := OnedataAccessToken,
    <<"adminId">> := AdminId
}, _Helper, OAuth2IdP) ->
    TokenAuth = auth_manager:build_token_auth(
        OnedataAccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(
        AdminId, TokenAuth, OAuth2IdP
    ),
    AdminCtx2 = maps:remove(<<"onedataAccessToken">>, AdminCtx),
    {ok, AdminCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }};
fill_in_webdav_oauth2_token(UserId, SessionId, UserCtx, #helper{
    insecure = false
}, OAuth2IdP) ->
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(UserId, SessionId, OAuth2IdP),
    UserCtx2 = maps:remove(<<"onedataAccessToken">>, UserCtx),
    {ok, UserCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }};
fill_in_webdav_oauth2_token(_UserId, _SessionId, AdminCtx = #{
    <<"onedataAccessToken">> := OnedataAccessToken,
    <<"adminId">> := AdminId
}, #helper{insecure = true}, OAuth2IdP) ->
    TokenAuth = auth_manager:build_token_auth(
        OnedataAccessToken, undefined, undefined,
        undefined, disallow_data_access_caveats
    ),
    {ok, {IdPAccessToken, TTL}} = idp_access_token:acquire(
        AdminId, TokenAuth, OAuth2IdP
    ),
    AdminCtx2 = maps:remove(<<"onedataAccessToken">>, AdminCtx),
    {ok, AdminCtx2#{
        <<"accessToken">> => IdPAccessToken,
        <<"accessTokenTTL">> => integer_to_binary(TTL)
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Queries external, third party LUMA service for the user context if enabled.
%% Fails with an error if the response is erroneous.
%% @end
%%--------------------------------------------------------------------
-spec fetch_user_ctx(session:id(), od_user:id(), od_group:id() | undefined,
    od_space:id(), storage:record(), helpers:helper()) ->
    {ok, user_ctx()} | {error, Reason :: term()} | undefined.
fetch_user_ctx(SessionId, UserId, _GroupId, SpaceId, Storage,
    Helper = #helper{name = ?WEBDAV_HELPER_NAME}
) ->
    fetch_user_ctx(SessionId, UserId, SpaceId, Storage, Helper);
fetch_user_ctx(SessionId, UserId, GroupId, SpaceId, Storage, Helper) ->
    case fetch_user_ctx(SessionId, UserId, SpaceId, Storage, Helper) of
        {ok, UserCtx} ->
            Result = luma_proxy:get_group_ctx(GroupId, SpaceId, Storage, Helper),
            case Result of
                {ok, GroupCtx} ->
                    {ok, maps:merge(UserCtx, GroupCtx)};
                Error ->
                    ?warning_stacktrace("Fetching user_ctx from LUMA failed with ~p",
                        [Error]),
                    {ok, GroupCtx} = maybe_generate_group_ctx(UserId, GroupId,
                        SpaceId, Helper#helper.name),
                    {ok, maps:merge(UserCtx, GroupCtx)}
            end;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% For the POSIX compatible storage generates user context (UID and GID) as a hash of
%% respectively user ID and space ID.
%% For the other storage returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec maybe_generate_user_ctx(od_user:id(), od_group:id() | od_space:id(), helper:name()) ->
    {ok, user_ctx()} | undefined.
maybe_generate_user_ctx(UserId, SpaceId, HelperName)
    when HelperName =:= ?POSIX_HELPER_NAME
    orelse HelperName =:= ?GLUSTERFS_HELPER_NAME
    orelse HelperName =:= ?NULL_DEVICE_HELPER_NAME
    ->
    luma:generate_user_ctx(UserId, SpaceId);
maybe_generate_user_ctx(_UserId, _SpaceId, _HelperName) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% For the POSIX storage generates user context (UID and GID) as a hash of
%% respectively user ID and space/group ID.
%% For the other storage returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec maybe_generate_user_ctx(od_user:id(), od_group:id() | undefined, od_space:id(),
    helper:name()) -> {ok, user_ctx()} | undefined.
maybe_generate_user_ctx(?ROOT_USER_ID, _GroupId, SpaceId, HelperName) ->
    maybe_generate_user_ctx(?ROOT_USER_ID, SpaceId, HelperName);
maybe_generate_user_ctx(UserId, undefined, SpaceId, HelperName) ->
    maybe_generate_user_ctx(UserId, SpaceId, HelperName);
maybe_generate_user_ctx(UserId, GroupId, _SpaceId, HelperName) ->
    maybe_generate_user_ctx(UserId, GroupId, HelperName).


%%--------------------------------------------------------------------
%% @doc
%% For the POSIX storage generates group context (GID) as a hash of
%% group ID or space ID (if group ID is undefined).
%% For the other storage returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec maybe_generate_group_ctx(od_user:id(), od_group:id() | undefined, od_space:id(),
    helper:name()) -> {ok, group_ctx()} | undefined.
maybe_generate_group_ctx(UserId, GroupId, SpaceId, HelperName)
    when HelperName =:= ?POSIX_HELPER_NAME
    orelse HelperName =:= ?GLUSTERFS_HELPER_NAME
    orelse HelperName =:= ?NULL_DEVICE_HELPER_NAME
    ->
    luma:generate_group_ctx(UserId, GroupId, SpaceId);
maybe_generate_group_ctx(_UserId, _GroupId, _SpaceId, _HelperName) ->
    undefined.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Generates group context  as a hash of given Id.
%% @end
%%-------------------------------------------------------------------
-spec generate_posix_group_ctx(od_space:id() | od_group:id()) -> {ok, group_ctx()}.
generate_posix_group_ctx(Id) ->
    {ok, GidRange} = application:get_env(?APP_NAME, luma_posix_gid_range),
    Gid = generate_posix_identifier(Id, GidRange),
    {ok, #{
        <<"gid">> => integer_to_binary(Gid)
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates POSIX storage identifier (UID, GID) as a hash of user ID or space ID.
%% @end
%%--------------------------------------------------------------------
-spec generate_posix_identifier(od_user:id() | od_space:id(),
    Range :: {non_neg_integer(), non_neg_integer()}) -> non_neg_integer().
generate_posix_identifier(?ROOT_USER_ID, _) ->
    0;
generate_posix_identifier(Id, {Low, High}) ->
    PosixId = crypto:bytes_to_integer(Id),
    Low + (PosixId rem (High - Low)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects POSIX storage for the list of configured space storages.
%% @end
%%--------------------------------------------------------------------
-spec select_posix_compatible_storage(od_space:id() | [od_storage:id()]) ->
    {ok, storage:record()} | {error, Reason :: term()}.
select_posix_compatible_storage(SpaceId) when is_binary(SpaceId) ->
    case space_logic:get_local_storage_ids(SpaceId) of
        {ok, StorageIds} ->
            case select_posix_compatible_storage(StorageIds) of
                undefined -> {error, not_found};
                Storage -> {ok, Storage}
            end;
        Error = {error, _} ->
            Error
    end;
select_posix_compatible_storage(StorageIds) when is_list(StorageIds)->
    lists:foldl(fun
        (StorageId, undefined) ->
            case storage:get(StorageId) of
                {ok, SR} ->
                    Helper = storage:get_helper(SR),
                    HelperName = helper:get_name(Helper),
                    case lists:member(HelperName, ?POSIX_COMPATIBLE_HELPERS) of
                        true -> SR;
                        false -> undefined
                    end;
                {error, not_found} ->
                    undefined
            end;
        (_, PosixCompatibleSR) ->
            PosixCompatibleSR
    end, undefined, StorageIds).
