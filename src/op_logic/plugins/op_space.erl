%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to space aspects such as:
%%% - space,
%%% - views,
%%% - transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(op_space).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([op_logic_plugin/0]).
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_VIEW_LIST_LIMIT, 100).
-define(DEFAULT_TRANSFER_LIST_LIMIT, 100).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_space.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, {view, _}, private) -> true;
operation_supported(create, {view_reduce_function, _}, private) -> true;

operation_supported(get, list, private) -> true;
operation_supported(get, instance, private) -> true;
operation_supported(get, views, private) -> true;
operation_supported(get, {view, _}, private) -> true;
operation_supported(get, {query_view, _}, private) -> true;
operation_supported(get, transfers, private) -> true;

operation_supported(update, {view, _}, private) -> true;

operation_supported(delete, {view, _}, private) -> true;
operation_supported(delete, {view_reduce_function, _}, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = {view, _}}}) -> #{
    required => #{
        <<"application/javascript">> => {binary, non_empty}
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
                    lists:all(fun(ProvId) -> is_binary(ProvId) end, Providers);
                (_) ->
                    false
            end
        }
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = {view_reduce_function, _}}}) -> #{
    required => #{<<"application/javascript">> => {binary, non_empty}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = list}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = views}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 0, ?MAX_LIST_LIMIT}},
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
        <<"key">> => {binary, any},
        <<"keys">> => {binary, any},
        <<"bbox">> => {binary, any}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = transfers}}) -> #{
    optional => #{
        <<"state">> => {binary, [<<"waiting">>, <<"ongoing">>, <<"ended">>]},
        <<"limit">> => {integer, {between, 0, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = update, gri = #gri{aspect = {view, _}}}) -> #{
    optional => #{
        <<"application/javascript">> => {binary, any},
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    {true, [ProviderId]};
                (Providers) when is_list(Providers) ->
                    lists:all(fun(ProvId) -> is_binary(ProvId) end, Providers);
                (_) ->
                    false
            end
        }
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = {view, _}}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = {view_reduce_function, _}}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:versioned_entity()} | op_logic:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), op_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    % User is always authorized to list his spaces
    true;

authorize(#op_req{operation = get, auth = Auth, gri = #gri{
    id = SpaceId,
    aspect = instance
}}, _) ->
    op_logic_utils:is_eff_space_member(Auth, SpaceId);

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

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = transfers
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{operation = update, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_VIEWS).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, data = Data, gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId),

    % In case of undefined `providers[]` local provider is chosen instead
    case maps:get(<<"providers[]">>, Data, undefined) of
        undefined ->
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                op_logic_utils:assert_space_supported_by(SpaceId, ProviderId)
            end, Providers)
    end;

validate(#op_req{operation = create, gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    % User spaces are listed by fetching information from zone,
    % whether they are supported locally is irrelevant.
    ok;

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = views}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {view, _}}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {query_view, _}}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = transfers}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{
    id = SpaceId,
    aspect = {view, _}
}} = Req, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId),

    % In case of undefined `providers[]` local provider is chosen instead
    case maps:get(<<"providers[]">>, Req#op_req.data, undefined) of
        undefined ->
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                op_logic_utils:assert_space_supported_by(SpaceId, ProviderId)
        end, Providers)
    end;

validate(#op_req{operation = delete, gri = #gri{id = SpaceId, aspect = {view, _}}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{
    id = SpaceId,
    aspect = {view_reduce_function, _}
}}, _) ->
    op_logic_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    index:save(
        SpaceId, ViewName,
        maps:get(<<"application/javascript">>, Data), undefined,
        prepare_view_options(Data),
        maps:get(<<"spatial">>, Data, false),
        maps:get(<<"providers[]">>, Data, [oneprovider:get_id()])
    );

create(#op_req{gri = #gri{id = SpaceId, aspect = {view_reduce_function, ViewName}}} = Req) ->
    ReduceFunction = maps:get(<<"application/javascript">>, Req#op_req.data),
    case index:update_reduce_function(SpaceId, ViewName, ReduceFunction) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{auth = ?USER(UserId, SessionId), gri = #gri{aspect = list}}, _) ->
    case user_logic:get_eff_spaces(SessionId, UserId) of
        {ok, EffSpaces} ->
            {ok ,lists:map(fun(SpaceId) ->
                {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
                #{<<"spaceId">> => SpaceId, <<"name">> => SpaceName}
            end, EffSpaces)};
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    case space_logic:get(Auth#auth.session_id, SpaceId) of
        {ok, #document{value = Space}} ->
            {ok, Space};
        {error, _} = Error ->
            Error
    end;

get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = views}}, _) ->
    PageToken = maps:get(<<"page_token">>, Data, <<"null">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_VIEW_LIST_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    case index:list(SpaceId, StartId, Offset, Limit) of
        {ok, Views} ->
            NextPageToken = case length(Views) of
                Limit -> #{<<"nextPageToken">> => lists:last(Views)};
                _ -> #{}
            end,
            {ok, maps:merge(#{
                <<"indexes">> => Views,   % TODO VFS-5608
                <<"views">> => Views
            }, NextPageToken)};
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
        {ok, {Rows}} ->
            QueryResult = lists:map(fun(Row) -> maps:from_list(Row) end, Rows),
            {ok, QueryResult};
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>);
        {error, _} = Error ->
            Error
    end;

get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = transfers}}, _) ->
    PageToken = maps:get(<<"page_token">>, Data, <<"null">>),
    TransferState = maps:get(<<"state">>, Data, <<"ongoing">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_TRANSFER_LIST_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    {ok, Transfers} = case TransferState of
        ?WAITING_TRANSFERS_STATE ->
            transfer:list_waiting_transfers(SpaceId, StartId, Offset, Limit);
        ?ONGOING_TRANSFERS_STATE ->
            transfer:list_ongoing_transfers(SpaceId, StartId, Offset, Limit);
        ?ENDED_TRANSFERS_STATE ->
            transfer:list_ended_transfers(SpaceId, StartId, Offset, Limit)
    end,

    NextPageToken = case length(Transfers) of
        Limit ->
            {ok, LinkKey} = transfer:get_link_key_by_state(
                lists:last(Transfers), TransferState
            ),
            #{<<"nextPageToken">> => LinkKey};
        _ ->
            #{}
    end,
    {ok, maps:merge(#{<<"transfers">> => Transfers}, NextPageToken)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {view, ViewName}}}) ->
    MapFunctionRaw = maps:get(<<"application/javascript">>, Data),
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
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
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