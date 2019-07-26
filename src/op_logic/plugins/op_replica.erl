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
-include_lib("ctool/include/api_errors.hrl").
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


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_replica.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, instance, private) -> true;
operation_supported(create, replicate_by_index, private) -> true;

operation_supported(get, distribution, private) -> true;

operation_supported(delete, instance, private) -> true;
operation_supported(delete, evict_by_index, private) -> true;

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

data_spec(#op_req{operation = create, gri = #gri{aspect = replicate_by_index}}) -> #{
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

data_spec(#op_req{operation = delete, gri = #gri{aspect = evict_by_index}}) -> #{
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
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_SCHEDULE_REPLICATION);

authorize(#op_req{operation = create, auth = ?USER(UserId), data = Data, gri = #gri{
    aspect = replicate_by_index
}}, _) ->
    SpaceId = maps:get(<<"space_id">>, Data),
    space_logic:has_eff_privileges(
        SpaceId, UserId, [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_INDICES]
    );

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = distribution}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.auth, SpaceId);

authorize(#op_req{operation = delete, auth = ?USER(UserId), data = Data, gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined ->
            % only eviction
            space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_SCHEDULE_EVICTION);
        _ ->
            % migration (eviction preceded by replication)
            space_logic:has_eff_privileges(
                SpaceId, UserId,
                [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION]
            )
    end;

authorize(#op_req{operation = delete, auth = ?USER(UserId), data = Data, gri = #gri{
    aspect = evict_by_index
}}, _) ->
    SpaceId = maps:get(<<"space_id">>, Data),
    case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined ->
            % only eviction
            space_logic:has_eff_privileges(SpaceId, UserId, [
                ?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_INDICES
            ]);
        _ ->
            % migration (eviction preceded by replication)
            space_logic:has_eff_privileges(SpaceId, UserId, [
                ?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION,
                ?SPACE_QUERY_INDICES
            ])
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId),

    op_logic_utils:assert_file_exists(Req#op_req.auth, Guid),

    % If `provider_id` is not specified local provider is chosen as target instead
    case maps:get(<<"provider_id">>, Req#op_req.data, undefined) of
        undefined ->
            ok;
        ReplicatingProvider ->
            op_logic_utils:assert_space_supported_by(SpaceId, ReplicatingProvider)
    end;

validate(#op_req{operation = create, gri = #gri{id = Name, aspect = replicate_by_index}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),
    op_logic_utils:assert_space_supported_locally(SpaceId),

    % If `provider_id` is not specified local provider is chosen as target instead
    case maps:get(<<"provider_id">>, Data, undefined) of
        undefined ->
            assert_index_exists_on_provider(SpaceId, Name, oneprovider:get_id());
        ReplicatingProvider ->
            op_logic_utils:assert_space_supported_by(SpaceId, ReplicatingProvider),
            assert_index_exists_on_provider(SpaceId, Name, ReplicatingProvider)
    end;

validate(#op_req{operation = get, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:assert_space_supported_locally(SpaceId),

    op_logic_utils:assert_file_exists(Req#op_req.auth, Guid),

    % If `provider_id` is not specified local provider is chosen as target instead
    case maps:get(<<"provider_id">>, Data, undefined) of
        undefined ->
            ok;
        EvictingProvider ->
            op_logic_utils:assert_space_supported_by(SpaceId, EvictingProvider)
    end,

    case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined ->
            ok;
        ReplicatingProvider ->
            op_logic_utils:assert_space_supported_by(SpaceId, ReplicatingProvider)
    end;

validate(#op_req{operation = delete, gri = #gri{id = Name, aspect = evict_by_index}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),
    op_logic_utils:assert_space_supported_locally(SpaceId),

    % If `provider_id` is not specified local provider is chosen as target instead
    case maps:get(<<"provider_id">>, Data, undefined) of
        undefined ->
            assert_index_exists_on_provider(SpaceId, Name, oneprovider:get_id());
        EvictingProvider ->
            op_logic_utils:assert_space_supported_by(SpaceId, EvictingProvider),
            assert_index_exists_on_provider(SpaceId, Name, EvictingProvider)
    end,

    case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined ->
            ok;
        ReplicatingProvider ->
            op_logic_utils:assert_space_supported_by(SpaceId, ReplicatingProvider),
            assert_index_exists_on_provider(SpaceId, Name, ReplicatingProvider)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{id = FileGuid, aspect = instance}}) ->
    case lfm:schedule_file_replication(
        Auth#auth.session_id,
        {guid, FileGuid},
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"url">>, Data, undefined)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

create(#op_req{auth = Auth, data = Data, gri = #gri{id = IndexName, aspect = replicate_by_index}}) ->
    case lfm:schedule_replication_by_index(
        Auth#auth.session_id,
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"url">>, Data, undefined),
        maps:get(<<"space_id">>, Data),
        IndexName,
        index_utils:sanitize_query_options(Data)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SessionId = Auth#auth.session_id,
    case lfm:get_file_distribution(SessionId, {guid, FileGuid}) of
        {ok, _Blocks} = Res ->
            Res;
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


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
    case lfm:schedule_replica_eviction(
        Auth#auth.session_id,
        {guid, FileGuid},
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"migration_provider_id">>, Data, undefined)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

delete(#op_req{auth = Auth, data = Data, gri = #gri{id = IndexName, aspect = evict_by_index}}) ->
    case lfm:schedule_replica_eviction_by_index(
        Auth#auth.session_id,
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"migration_provider_id">>, Data, undefined),
        maps:get(<<"space_id">>, Data),
        IndexName,
        index_utils:sanitize_query_options(Data)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_index_exists_on_provider(od_space:id(), index:name(),
    od_provider:id()) -> ok | no_return().
assert_index_exists_on_provider(SpaceId, IndexName, ProviderId) ->
    case index:exists_on_provider(SpaceId, IndexName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_INDEX_NOT_EXISTS_ON(ProviderId))
    end.
