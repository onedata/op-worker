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
-export([check_and_cache_result/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check rule using cache, or compute it and cache result.n
%% @end
%%--------------------------------------------------------------------
-spec check_and_cache_result(check_permissions:raw_access_definition(), user_ctx:ctx(), file_ctx:ctx()) -> ok.
check_and_cache_result(Definition, UserCtx, DefaultFileCtx) ->
    try
        case Definition of
            {Type, SubjectCtx} ->
                case permission_in_cache(Type, UserCtx, SubjectCtx) of
                    true ->
                        ok;
                    false ->
                        ok = rules:check(Definition, UserCtx, DefaultFileCtx),
                        cache_permission(Type, UserCtx, SubjectCtx, ok)
                end;
            Type ->
                case permission_in_cache(Type, UserCtx, DefaultFileCtx) of
                    true ->
                        ok;
                    false ->
                        ok = rules:check(Definition, UserCtx, DefaultFileCtx),
                        cache_permission(Type, UserCtx, DefaultFileCtx, ok)
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

%%%===================================================================
%%% Internal functions
%%%===================================================================