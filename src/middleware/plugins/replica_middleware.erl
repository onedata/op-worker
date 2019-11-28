%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to file aspects such as:
%%% - distribution,
%%% - replication,
%%% - eviction,
%%% - migration.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
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
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, instance, private) -> true;
operation_supported(create, replicate_by_view, private) -> true;

operation_supported(get, distribution, private) -> true;

operation_supported(delete, instance, private) -> true;
operation_supported(delete, evict_by_view, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
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
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    authorize_transfer_creation(SpaceId, UserId, replication, false);

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    aspect = replicate_by_view
}} = OpReq, _) ->
    SpaceId = maps:get(<<"space_id">>, OpReq#op_req.data),
    authorize_transfer_creation(SpaceId, UserId, replication, true);

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = distribution}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    middleware_utils:is_eff_space_member(Req#op_req.auth, SpaceId);

authorize(#op_req{operation = delete, auth = ?USER(UserId), data = Data, gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),

    TransferType = case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined -> eviction;
        _ -> migration
    end,
    authorize_transfer_creation(SpaceId, UserId, TransferType, false);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    aspect = evict_by_view
}} = OpReq, _) ->
    Data = OpReq#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),

    TransferType = case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined -> eviction;
        _ -> migration
    end,
    authorize_transfer_creation(SpaceId, UserId, TransferType, true).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, auth = Auth, data = Data, gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    validate_transfer_creation(
        Auth,
        file_id:guid_to_space_id(Guid),
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        undefined,
        {guid, Guid}
    );

validate(#op_req{operation = create, auth = Auth, data = Data, gri = #gri{
    id = Name,
    aspect = replicate_by_view
}}, _) ->
    validate_transfer_creation(
        Auth,
        maps:get(<<"space_id">>, Data),
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        undefined,
        {view, Name}
    );

validate(#op_req{operation = get, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, auth = Auth, data = Data, gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    validate_transfer_creation(
        Auth,
        file_id:guid_to_space_id(Guid),
        maps:get(<<"migration_provider_id">>, Data, undefined),
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        {guid, Guid}
    );

validate(#op_req{operation = delete, auth = Auth, data = Data, gri = #gri{
    id = Name,
    aspect = evict_by_view
}}, _) ->
    validate_transfer_creation(
        Auth,
        maps:get(<<"space_id">>, Data),
        maps:get(<<"migration_provider_id">>, Data, undefined),
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        {view, Name}
    ).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
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
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    ?check(lfm:get_file_distribution(Auth#auth.session_id, {guid, FileGuid})).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec authorize_transfer_creation(od_space:id(), od_user:id(),
    transfer:type(), TransferByIndex :: boolean()) -> boolean().
authorize_transfer_creation(SpaceId, UserId, replication, false) ->
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_transfer_creation(SpaceId, UserId, replication, true) ->
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION, ?SPACE_QUERY_VIEWS],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_transfer_creation(SpaceId, UserId, eviction, false) ->
    RequiredPrivs = [?SPACE_SCHEDULE_EVICTION],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_transfer_creation(SpaceId, UserId, eviction, true) ->
    RequiredPrivs = [?SPACE_SCHEDULE_EVICTION, ?SPACE_QUERY_VIEWS],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_transfer_creation(SpaceId, UserId, migration, false) ->
    RequiredPrivs = [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs);
authorize_transfer_creation(SpaceId, UserId, migration, true) ->
    RequiredPrivs = [
        ?SPACE_SCHEDULE_REPLICATION,
        ?SPACE_SCHEDULE_EVICTION,
        ?SPACE_QUERY_VIEWS
    ],
    space_logic:has_eff_privileges(SpaceId, UserId, RequiredPrivs).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates whether transfer can be created/scheduled. That includes:
%% - check whether space is supported locally (scheduling provider)
%%   and by replicating and evicting providers,
%% - check if file exists in case of transfer of file/dir,
%% - check if index exists on replicating and evicting providers
%%   in case of transfer by index.
%% Depending on transfer type (replication, eviction, migration) either
%% ReplicatingProvider or EvictingProvider should be left 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec validate_transfer_creation(
    Auth :: aai:auth(),
    SpaceId :: od_space:id(),
    ReplicatingProvider :: undefined | od_provider:id(),
    EvictingProvider :: undefined | od_provider:id(),
    TransferBy :: {guid, file_id:file_guid()} | {view, index:name()}
) ->
    ok | no_return().
validate_transfer_creation(Auth, SpaceId, ReplicatingProvider, EvictingProvider, {guid, Guid}) ->
    middleware_utils:assert_space_supported_locally(SpaceId),
    middleware_utils:assert_file_exists(Auth, Guid),

    assert_space_supported_by(SpaceId, ReplicatingProvider),
    assert_space_supported_by(SpaceId, EvictingProvider);
validate_transfer_creation(_Auth, SpaceId, ReplicatingProvider, EvictingProvider, {view, Name}) ->
    middleware_utils:assert_space_supported_locally(SpaceId),

    assert_space_supported_by(SpaceId, ReplicatingProvider),
    assert_view_exists_on_provider(SpaceId, Name, ReplicatingProvider),

    assert_space_supported_by(SpaceId, EvictingProvider),
    assert_view_exists_on_provider(SpaceId, Name, EvictingProvider).


%% @private
-spec assert_space_supported_by(od_space:id(), undefined | od_provider:id()) ->
    ok | no_return().
assert_space_supported_by(_SpaceId, undefined) ->
    ok;
assert_space_supported_by(SpaceId, ProviderId) ->
    middleware_utils:assert_space_supported_by(SpaceId, ProviderId).


%% @private
-spec assert_view_exists_on_provider(od_space:id(), index:name(),
    undefined | od_provider:id()) -> ok | no_return().
assert_view_exists_on_provider(_SpaceId, _ViewName, undefined) ->
    ok;
assert_view_exists_on_provider(SpaceId, ViewName, ProviderId) ->
    case index:exists_on_provider(SpaceId, ViewName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_VIEW_NOT_EXISTS_ON(ProviderId))
    end.
