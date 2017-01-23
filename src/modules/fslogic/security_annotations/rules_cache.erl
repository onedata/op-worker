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
-export([check_and_cache_results/3]).

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
%% Check rule using cache, or compute it and cache result. Returns updated
%% default file context.
%% @end
%%--------------------------------------------------------------------
-spec check_and_cache_result(check_permissions:access_definition(),
    user_ctx:ctx(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
check_and_cache_result(Definition, UserCtx, DefaultFileCtx) ->
    Type = get_type(Definition),
    SubjectCtx = get_subject(Definition, DefaultFileCtx),
    try
        case permission_in_cache(Type, UserCtx, SubjectCtx) of
            true ->
                {ok, DefaultFileCtx};
            false ->
                {ok, DefaultFileCtx2} = rules:check_normal_or_default_def(Definition, UserCtx, DefaultFileCtx),
                cache_permission(Type, UserCtx, SubjectCtx, ok),
                {ok, DefaultFileCtx2}
        end
    catch
        _:?EACCES ->
            cache_permission(Type, UserCtx, SubjectCtx, ?EACCES),
            throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns type of permission check from its definition.
%% @end
%%--------------------------------------------------------------------
-spec get_type(check_permissions:access_definition()) -> check_permissions:check_type().
get_type({Type, _}) ->
    Type;
get_type(Type) ->
    Type.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns file context related to given access definition.
%% @end
%%--------------------------------------------------------------------
-spec get_subject(check_permissions:access_definition(), file_ctx:ctx()) ->
    file_ctx:ctx().
get_subject({_Type, FileCtx}, _) ->
    FileCtx;
get_subject(_, DefaultFileCtx) ->
    DefaultFileCtx.

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
%% Caches result for given definition.
%% @end
%%--------------------------------------------------------------------
-spec cache_permission(check_permissions:check_type(), user_ctx:ctx(), file_ctx:ctx(), ok | ?EACCES) ->
    {ok, term()} | calculate | no_return().
cache_permission(CheckType, UserCtx, FileCtx, Value) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    permissions_cache:cache_permission({CheckType, UserId, Guid}, Value).
