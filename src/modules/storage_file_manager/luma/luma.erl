%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
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

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-type user_ctx() :: helper:user_ctx().
-type posix_user_ctx() :: {Uid :: non_neg_integer(), Gid :: non_neg_integer()}.

-export_type([user_ctx/0, posix_user_ctx/0]).

%% API
-export([get_server_user_ctx/4, get_client_user_ctx/4, get_posix_user_ctx/2]).


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
-spec get_server_user_ctx(od_user:id(), od_space:id(), storage:doc(), helper:name()) ->
    {ok, user_ctx()} | {error, Reason :: term()}.
get_server_user_ctx(UserId, SpaceId, StorageDoc, HelperName) ->
    case storage:select_helper(StorageDoc, HelperName) of
        {ok, Helper} ->
            get_user_ctx([
                {fun get_admin_ctx/2, [UserId, Helper]},
                {fun fetch_user_ctx/4, [UserId, SpaceId, StorageDoc, Helper]},
                {fun generate_user_ctx/3, [UserId, SpaceId, HelperName]},
                {fun get_admin_ctx/2, [?ROOT_USER_ID, Helper]}
            ]);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage user context associated with the chosen storage helper,
%% which is appropriate for the remote client operations working in direct IO
%% mode. First an external, third party LUMA service is queried. Next, if the
%% storage helper is defined as insecure, storage admin context is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_client_user_ctx(od_user:id(), od_space:id(), storage:doc(), helper:name()) ->
    {ok, user_ctx()} | {error, Reason :: term()}.
get_client_user_ctx(UserId, SpaceId, StorageDoc, HelperName) ->
    case storage:select_helper(StorageDoc, HelperName) of
        {ok, Helper} ->
            get_user_ctx([
                {fun fetch_user_ctx/4, [UserId, SpaceId, StorageDoc, Helper]},
                {fun get_insecure_user_ctx/1, [Helper]},
                {fun get_nobody_ctx/1, [Helper]}
            ]);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% For a space supported by a POSIX storage returns POSIX user context
%% (UID and GID), otherwise generates it.
%% @end
%%--------------------------------------------------------------------
-spec get_posix_user_ctx(od_user:id(), od_space:id()) -> posix_user_ctx().
get_posix_user_ctx(UserId, SpaceId) ->
    {ok, UserCtx} = case select_posix_storage(SpaceId) of
        {ok, StorageDoc} ->
            luma:get_server_user_ctx(UserId, SpaceId, StorageDoc, ?POSIX_HELPER_NAME);
        {error, {not_found, _}} ->
            generate_user_ctx(UserId, SpaceId, ?POSIX_HELPER_NAME)
    end,
    #{<<"uid">> := Uid, <<"gid">> := Gid} = UserCtx,
    {binary_to_integer(Uid), binary_to_integer(Gid)}.


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
%% For the root user returns storage admin context, otherwise 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec get_admin_ctx(od_user:id(), storage:helper()) -> {ok, user_ctx()} | undefined.
get_admin_ctx(?ROOT_USER_ID, #helper{name = ?POSIX_HELPER_NAME}) ->
    {ok, helper:new_posix_user_ctx(0, 0)};
get_admin_ctx(?ROOT_USER_ID, #helper{admin_ctx = AdminCtx}) ->
    {ok, AdminCtx};
get_admin_ctx(_UserId, _Helper) ->
    undefined.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For the insecure storage helper returns storage admin context, otherwise
%% 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec get_insecure_user_ctx(storage:helper()) -> {ok, user_ctx()} | undefined.
get_insecure_user_ctx(#helper{name = ?POSIX_HELPER_NAME}) ->
    undefined;
get_insecure_user_ctx(#helper{insecure = true, admin_ctx = AdminCtx}) ->
    {ok, AdminCtx};
get_insecure_user_ctx(_Helper) ->
    undefined.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For the POSIX storage helper returns nobody user context, otherwise 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec get_nobody_ctx(storage:helper()) -> {ok, user_ctx()} | undefined.
get_nobody_ctx(#helper{name = ?POSIX_HELPER_NAME}) ->
    {ok, helper:new_posix_user_ctx(-1, -1)};
get_nobody_ctx(_Helper) ->
    undefined.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Queries external, third party LUMA service for the user context if enabled.
%% Fails with an error if the response is erroneous.
%% @end
%%--------------------------------------------------------------------
-spec fetch_user_ctx(od_user:id(), od_space:id(), storage:doc(), storage:helper()) ->
    {ok, user_ctx()} | {error, Reason :: term()} | undefined.
fetch_user_ctx(UserId, SpaceId, StorageDoc, Helper) ->
    case storage:is_luma_enabled(StorageDoc) of
        false ->
            undefined;
        true ->
            LumaConfig = storage:get_luma_config(StorageDoc),
            LumaCacheTimeout = luma_config:get_timeout(LumaConfig),
            Result = luma_cache:get(UserId,
                fun luma_proxy:get_user_ctx/4,
                [UserId, SpaceId, StorageDoc, Helper],
                LumaCacheTimeout
            ),

            case Result of
                {error, Reason} ->
                    {error, {luma_server, Reason}};
                Other ->
                    Other
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For the POSIX storage generates user context (UID and GID) as a hash of
%% respectively user ID and space ID. For the other storage returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec generate_user_ctx(od_user:id(), od_space:id(), helper:name()) ->
    {ok, user_ctx()} | undefined.
generate_user_ctx(?ROOT_USER_ID, _, ?POSIX_HELPER_NAME) ->
    {ok, #{<<"uid">> => <<"0">>, <<"gid">> => <<"0">>}};
generate_user_ctx(UserId, SpaceId, ?POSIX_HELPER_NAME) ->
    {ok, UidRange} = application:get_env(?APP_NAME, luma_posix_uid_range),
    {ok, GidRange} = application:get_env(?APP_NAME, luma_posix_gid_range),
    Uid = generate_posix_identifier(UserId, UidRange),
    Gid = generate_posix_identifier(SpaceId, GidRange),
    {ok, #{
        <<"uid">> => integer_to_binary(Uid),
        <<"gid">> => integer_to_binary(Gid)
    }};
generate_user_ctx(_UserId, _SpaceId, _HelperName) ->
    undefined.

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
-spec select_posix_storage(od_space:id()) ->
    {ok, storage:doc()} | {error, Reason :: term()}.
select_posix_storage(SpaceId) ->
    StorageIds = case space_storage:get(SpaceId) of
        {ok, Doc} -> space_storage:get_storage_ids(Doc);
        {error, {not_found, _}} -> []
    end,
    StorageDocs = lists:filtermap(fun(StorageId) ->
        case storage:get(StorageId) of
            {ok, StorageDoc} ->
                case storage:select_helper(StorageDoc, ?POSIX_HELPER_NAME) of
                    {ok, _} -> {true, StorageDoc};
                    {error, {not_found, _}} -> false
                end;
            {error, {not_found, _}} -> false
        end
    end, StorageIds),
    case StorageDocs of
        [] -> {error, {not_found, storage}};
        [StorageDoc | _] -> {ok, StorageDoc}
    end.