%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Wrapper for rules, caching its results.
%%% @end
%%%--------------------------------------------------------------------
-module(rules_cache).
-author("Tomasz Lichon").

-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([check_and_cache_result/3, check_and_cache_results/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check rules list using cache. Returns updated file context.
%% @end
%%--------------------------------------------------------------------
-spec check_and_cache_results(check_permissions:access_definition(),
    user_ctx:ctx(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
check_and_cache_results([], _UserCtx, DefaultFileCtx) ->
    {ok, DefaultFileCtx};
check_and_cache_results([Def | Rest], UserCtx, DefaultFileCtx) ->
    {ok, DefaultFileCtx2} = check_and_cache_result(Def, UserCtx, DefaultFileCtx),
    check_and_cache_results(Rest, UserCtx, DefaultFileCtx2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check rule using cache, or compute it and cache result. Returns updated file
%% context.
%% @end
%%--------------------------------------------------------------------
-spec check_and_cache_result(check_permissions:access_definition(),
    user_ctx:ctx(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
check_and_cache_result(Definition, UserCtx, DefaultFileCtx) ->
    try
        case Definition of %todo refactor duplicate code
            {Type, SubjectCtx} ->
                case permission_in_cache(Type, UserCtx, SubjectCtx) of
                    true ->
                        {ok, DefaultFileCtx};
                    false ->
                        {ok, DefaultFileCtx2} = rules:check_normal_or_default_def(Definition, UserCtx, DefaultFileCtx),
                        cache_permission(Type, UserCtx, SubjectCtx, ok),
                        {ok, DefaultFileCtx2}
                end;
            Type ->
                case permission_in_cache(Type, UserCtx, DefaultFileCtx) of
                    true ->
                        {ok, DefaultFileCtx};
                    false ->
                        {ok, DefaultFileCtx2} = rules:check_normal_or_default_def(Definition, UserCtx, DefaultFileCtx),
                        cache_permission(Type, UserCtx, DefaultFileCtx, ok),
                        {ok, DefaultFileCtx2}
                end
        end
    catch
        _:?EACCES ->
            case Definition of
                {Type_, SubjectCtx_} ->
                    cache_permission(Type_, UserCtx, SubjectCtx_, ?EACCES);
                Type_ ->
                    cache_permission(Type_, UserCtx, DefaultFileCtx, ?EACCES)
            end,
            throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns cached permission for given definition.
%% @end
%%--------------------------------------------------------------------
-spec permission_in_cache(check_permissions:check_type(), user_ctx:ctx(), file_ctx:ctx()) ->
    boolean() | no_return().
permission_in_cache(CheckType, UserCtx, FileCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    case permissions_cache:check_permission({CheckType, UserId, Guid}) of
        {ok, ok} ->
            true;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            false
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Caches result for given definition
%% @end
%%--------------------------------------------------------------------
-spec cache_permission(check_permissions:check_type(), user_ctx:ctx(), file_ctx:ctx(), ok | ?EACCES) ->
    {ok, term()} | calculate | no_return().
cache_permission(CheckType, UserCtx, FileCtx, Value) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    permissions_cache:cache_permission({CheckType, UserId, Guid}, Value).
