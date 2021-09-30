%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to space views aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_views_middleware_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = {view, _}}}) -> #{
    required => #{
        <<"mapFunction">> => {binary, non_empty}
    },
    optional => #{
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    {true, [ProviderId]};
                (Providers) when is_list(Providers) ->
                    lists:all(fun is_binary/1, Providers);
                (_) ->
                    false
            end
        }
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = {view_reduce_function, _}}}) -> #{
    required => #{<<"reduceFunction">> => {binary, non_empty}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = views}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = {view, _}}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = {query_view, _}}}) -> #{
    optional => #{
        <<"descending">> => {boolean, any},
        <<"limit">> => {integer, {not_lower_than, 1}},
        <<"skip">> => {integer, {not_lower_than, 1}},
        <<"stale">> => {binary, [<<"ok">>, <<"update_after">>, <<"false">>]},
        <<"spatial">> => {boolean, any},
        <<"inclusive_end">> => {boolean, any},
        <<"start_range">> => {binary, any},
        <<"end_range">> => {binary, any},
        <<"startkey">> => {binary, any},
        <<"endkey">> => {binary, any},
        <<"startkey_docid">> => {binary, any},
        <<"endkey_docid">> => {binary, any},
        <<"key">> => {binary, any},
        <<"keys">> => {binary, any},
        <<"bbox">> => {binary, any}
    }
};

data_spec(#op_req{operation = update, gri = #gri{aspect = {view, _}}}) -> #{
    optional => #{
        <<"mapFunction">> => {binary, any},
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    {true, [ProviderId]};
                (Providers) when is_list(Providers) ->
                    lists:all(fun is_binary/1, Providers);
                (_) ->
                    false
            end
        }
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = {As, _}}}) when
    As =:= view;
    As =:= view_reduce_function
->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {As, _}
}}, _) when
    As =:= view;
    As =:= view_reduce_function
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = views
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_VIEWS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_VIEWS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {query_view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_QUERY_VIEWS);

authorize(#op_req{operation = update, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {As, _}}
}, _) when
    As =:= view;
    As =:= view_reduce_function
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, data = Data, gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId),
    assert_space_supported_on_remote_providers(SpaceId, Data);

validate(#op_req{operation = create, gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = views}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {As, _}}}, _) when
    As =:= view;
    As =:= query_view
->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, data = Data, gri = #gri{id = SpaceId, aspect = {view, _}}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId),
    assert_space_supported_on_remote_providers(SpaceId, Data);

validate(#op_req{operation = delete, gri = #gri{id = SpaceId, aspect = {As, _}}}, _) when
    As =:= view;
    As =:= view_reduce_function
->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    index:save(
        SpaceId, ViewName,
        maps:get(<<"mapFunction">>, Data), undefined,
        prepare_view_options(Data),
        maps:get(<<"spatial">>, Data, false),
        maps:get(<<"providers[]">>, Data, [oneprovider:get_id()])
    );

create(#op_req{gri = #gri{id = SpaceId, aspect = {view_reduce_function, ViewName}}} = Req) ->
    ReduceFunction = maps:get(<<"reduceFunction">>, Req#op_req.data),

    case index:update_reduce_function(SpaceId, ViewName, ReduceFunction) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = views}}, _) ->
    PageToken = maps:get(<<"page_token">>, Data, <<"null">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    case index:list(SpaceId, StartId, Offset, Limit) of
        {ok, Views} ->
            NextPageToken = case length(Views) =:= Limit of
                true -> lists:last(Views);
                false -> null
            end,
            {ok, #{
                <<"views">> => Views,
                <<"nextPageToken">> => NextPageToken
            }};
        {error, _} = Error ->
            Error
    end;

get(#op_req{gri = #gri{id = SpaceId, aspect = {view, ViewName}}}, _) ->
    case index:get_json(SpaceId, ViewName) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end;

get(#op_req{gri = #gri{id = SpaceId, aspect = {query_view, ViewName}}} = Req, _) ->
    Options = view_utils:sanitize_query_options(Req#op_req.data),
    case index:query(SpaceId, ViewName, Options) of
        {ok, #{<<"rows">> := Rows}} ->
            {ok, Rows};
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    MapFunctionRaw = maps:get(<<"mapFunction">>, Data),
    MapFun = utils:ensure_defined(MapFunctionRaw, <<>>, undefined),

    case index:update(
        SpaceId, ViewName,
        MapFun,
        prepare_view_options(Data),
        maps:get(<<"spatial">>, Data, undefined),
        maps:get(<<"providers[]">>, Data, undefined)
    ) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    case index:delete(SpaceId, ViewName) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end;

delete(#op_req{gri = #gri{id = SpaceId, aspect = {view_reduce_function, ViewName}}}) ->
    case index:update_reduce_function(SpaceId, ViewName, undefined) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_space_supported_on_remote_providers(od_space:id(), middleware:data()) ->
    ok | no_return().
assert_space_supported_on_remote_providers(SpaceId, Data) ->
    case maps:get(<<"providers[]">>, Data, undefined) of
        undefined ->
            % In case of undefined `providers[]` local provider is chosen instead
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                middleware_utils:assert_space_supported_by(SpaceId, ProviderId)
            end, Providers)
    end.


%% @private
-spec prepare_view_options(map()) -> list().
prepare_view_options(Data) ->
    Options = case maps:get(<<"replica_update_min_changes">>, Data, undefined) of
        undefined ->
            [];
        ReplicaUpdateMinChanges ->
            [{replica_update_min_changes, ReplicaUpdateMinChanges}]
    end,

    case maps:get(<<"update_min_changes">>, Data, undefined) of
        undefined ->
            Options;
        UpdateMinChanges ->
            [{update_min_changes, UpdateMinChanges} | Options]
    end.
