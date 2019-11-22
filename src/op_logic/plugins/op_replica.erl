%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to file aspects such as:
%%% - distribution,
%%% - replication,
%%% - eviction,
%%% - migration.
%%% @end
%%%-------------------------------------------------------------------
-module(op_replica).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

-define(check_transfer_creation(__FunctionCall),
    case __FunctionCall of
        {ok, __TransferId} ->
            {ok, value, __TransferId};
        {error, __Errno} ->
            ?ERROR_POSIX(__Errno)
    end

).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), gri:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, instance, private) -> true;
operation_supported(create, replicate_by_view, private) -> true;

operation_supported(get, distribution, private) -> true;

operation_supported(delete, instance, private) -> true;
operation_supported(delete, evict_by_view, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"url">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = replicate_by_view}}) -> #{
    required => #{
        <<"space_id">> => {binary, non_empty}
    },
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"url">> => {binary, non_empty},
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

data_spec(#op_req{operation = get, gri = #gri{aspect = distribution}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"migration_provider_id">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = evict_by_view}}) -> #{
    required => #{
        <<"space_id">> => {binary, non_empty}
    },
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"migration_provider_id">> => {binary, non_empty},
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
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:versioned_entity()} | errors:error().
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
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    transfer_utils:authorize_creation(SpaceId, UserId, replication, false);

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    aspect = replicate_by_view
}} = OpReq, _) ->
    SpaceId = maps:get(<<"space_id">>, OpReq#op_req.data),
    transfer_utils:authorize_creation(SpaceId, UserId, replication, true);

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = distribution}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.auth, SpaceId);

authorize(#op_req{operation = delete, auth = ?USER(UserId), data = Data, gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),

    TransferType = case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined -> eviction;
        _ -> migration
    end,
    transfer_utils:authorize_creation(SpaceId, UserId, TransferType, false);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    aspect = evict_by_view
}} = OpReq, _) ->
    Data = OpReq#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),

    TransferType = case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined -> eviction;
        _ -> migration
    end,
    transfer_utils:authorize_creation(SpaceId, UserId, TransferType, true).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = file_id:guid_to_space_id(Guid),

    ReplicatingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
    transfer_utils:validate_creation(
        Req#op_req.auth, SpaceId,
        ReplicatingProvider, undefined, {guid, Guid}
    );

validate(#op_req{operation = create, gri = #gri{id = Name, aspect = replicate_by_view}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),

    ReplicatingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
    transfer_utils:validate_creation(
        Req#op_req.auth, SpaceId,
        ReplicatingProvider, undefined, {view, Name}
    );

validate(#op_req{operation = get, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = file_id:guid_to_space_id(Guid),

    EvictingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
    ReplicatingProvider = maps:get(<<"migration_provider_id">>, Data, undefined),
    transfer_utils:validate_creation(
        Req#op_req.auth, SpaceId,
        ReplicatingProvider, EvictingProvider, {guid, Guid}
    );

validate(#op_req{operation = delete, gri = #gri{id = Name, aspect = evict_by_view}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),

    EvictingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
    ReplicatingProvider = maps:get(<<"migration_provider_id">>, Data, undefined),
    transfer_utils:validate_creation(
        Req#op_req.auth, SpaceId,
        ReplicatingProvider, EvictingProvider, {view, Name}
    ).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = instance}}) ->
    ?check_transfer_creation(lfm:schedule_file_replication(
        Auth#auth.session_id,
        {guid, FileGuid},
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"url">>, Data, undefined)
    ));

create(#op_req{auth = Auth, data = Data, gri = #gri{id = ViewName, aspect = replicate_by_view}}) ->
    ?check_transfer_creation(lfm:schedule_replication_by_view(
        Auth#auth.session_id,
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"url">>, Data, undefined),
        maps:get(<<"space_id">>, Data),
        ViewName,
        view_utils:sanitize_query_options(Data)
    )).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    ?check(lfm:get_file_distribution(Auth#auth.session_id, {guid, FileGuid})).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = instance}}) ->
    ?check_transfer_creation(lfm:schedule_replica_eviction(
        Auth#auth.session_id,
        {guid, FileGuid},
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"migration_provider_id">>, Data, undefined)
    ));

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = ViewName, aspect = evict_by_view}}) ->
    ?check_transfer_creation(lfm:schedule_replica_eviction_by_view(
        Auth#auth.session_id,
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"migration_provider_id">>, Data, undefined),
        maps:get(<<"space_id">>, Data),
        ViewName,
        view_utils:sanitize_query_options(Data)
    )).
